package tech.ydb.yoj.databind.schema;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;
import tech.ydb.yoj.databind.schema.reflect.ReflectType;
import tech.ydb.yoj.databind.schema.reflect.Reflector;
import tech.ydb.yoj.databind.schema.reflect.StdReflector;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PROTECTED;

public abstract class Schema<T> {
    public static final String PATH_DELIMITER = ".";

    @Getter(PROTECTED)
    private final SchemaKey<T> schemaKey;

    @Getter
    private final List<JavaField> fields;

    @Getter
    private final List<Index> globalIndexes;

    @Getter
    @Nullable
    private final TtlModifier ttlModifier;

    @Getter
    private final List<Changefeed> changefeeds;

    protected final ReflectType<T> reflectType;

    private final String staticName;

    protected Schema(@NonNull Class<T> type) {
        this(type, StdReflector.instance);
    }

    protected Schema(@NonNull Class<T> type, @NonNull NamingStrategy namingStrategy) {
        this(type, namingStrategy, StdReflector.instance);
    }

    protected Schema(@NonNull Class<T> type, @NonNull Reflector reflector) {
        this(SchemaKey.of(type), reflector);
    }

    protected Schema(@NonNull Class<T> type, @NonNull NamingStrategy namingStrategy, @NonNull Reflector reflector) {
        this(SchemaKey.of(type, namingStrategy), reflector);
    }

    protected Schema(@NonNull SchemaKey<T> key, @NonNull Reflector reflector) {
        Class<T> type = key.clazz();
        NamingStrategy namingStrategy = key.namingStrategy();

        this.reflectType = reflector.reflectRootType(type);

        this.schemaKey = key;
        this.staticName = namingStrategy.getNameForClass(type);

        this.fields = reflectType.getFields().stream().map(this::newRootJavaField).toList();
        recurseFields(this.fields)
                .filter(f -> f.getName() == null)
                .forEachOrdered(namingStrategy::assignFieldName);
        validateFieldNames();

        this.globalIndexes = prepareIndexes(collectIndexes(type));
        this.ttlModifier = prepareTtlModifier(extractTtlModifier(type));
        this.changefeeds = prepareChangefeeds(collectChangefeeds(type));
    }
    
    protected Schema(Schema<?> schema, String subSchemaFieldPath) {
        JavaField subSchemaField = schema.getField(subSchemaFieldPath);

        @SuppressWarnings("unchecked") ReflectType<T> rt = (ReflectType<T>) subSchemaField.field.getReflectType();
        this.reflectType = rt;

        this.schemaKey = schema.schemaKey.withClazz(reflectType.getRawType());

        this.staticName = schema.staticName;
        this.globalIndexes = schema.globalIndexes;

        if (subSchemaField.fields != null) {
            this.fields = subSchemaField.fields.stream().map(this::newRootJavaField).toList();
        } else {
            if (subSchemaField.getCustomValueTypeInfo() != null) {
                var dummyField = new JavaField(new DummyCustomValueSubField(subSchemaField), subSchemaField, __ -> true);
                dummyField.setName(subSchemaField.getName());
                this.fields = List.of(dummyField);
            } else {
                this.fields = List.of();
            }
        }
        this.ttlModifier = schema.ttlModifier;
        this.changefeeds = schema.changefeeds;
    }

    public String getTypeName() {
        return getType().getSimpleName();
    }

    private void validateFieldNames() {
        flattenFields().stream().collect(toMap(JavaField::getName, Function.identity(), ((x, y) -> {
            throw new IllegalArgumentException("fields with same name `%s` detected: `{%s}` and `{%s}`"
                    .formatted(x.getName(), x.getField(), y.getField()));
        })));
    }

    private List<Index> prepareIndexes(List<GlobalIndex> indexes) {
        List<Index> outputIndexes = new ArrayList<>();
        Set<String> indexNames = new HashSet<>();
        for (GlobalIndex index : indexes) {
            String name = index.name();
            if (name.isBlank()) {
                throw new IllegalArgumentException(
                        format("index defined for %s has no name", getType()));
            }
            if (!indexNames.add(name)) {
                throw new IllegalArgumentException(
                        format("index with name \"%s\" already defined for %s", name, getType())
                );
            }
            var fieldPaths = index.fields();
            if (fieldPaths.length == 0) {
                throw new IllegalArgumentException(
                        format("index \"%s\" defined for %s has no fields", name, getType())
                );
            }
            List<String> columns = new ArrayList<>(fieldPaths.length);
            for (String fieldPath : fieldPaths) {
                var field = findField(fieldPath)
                        .orElseThrow(() -> new IllegalArgumentException(
                                format("index \"%s\" defined for %s tries to access unknown field \"%s\"",
                                        name, getType(), fieldPath)
                        ));
                if (!field.isFlat()) {
                    throw new IllegalArgumentException(
                            format("index \"%s\" defined for %s tries to access non-flat field \"%s\"",
                                    name, getType(), fieldPath));
                }
                columns.add(field.getName());
            }
            outputIndexes.add(new Index(name, List.copyOf(columns), index.type() == GlobalIndex.Type.UNIQUE));
        }
        return outputIndexes;
    }

    private TtlModifier prepareTtlModifier(TTL ttlAnnotation) {
        if (ttlAnnotation == null) {
            return null;
        }
        var fieldPath = ttlAnnotation.field();
        var field = getField(fieldPath);
        Preconditions.checkArgument(field.isFlat(),
                "ttl defined for %s tries to access non-flat field \"%s\"", getType(), fieldPath);

        var parsedInterval = Duration.parse(ttlAnnotation.interval());
        Preconditions.checkArgument(!(parsedInterval.isNegative() || parsedInterval.isZero()),
                "ttl value defined for %s must be positive", getType());
        return new TtlModifier(field.getName(), (int) parsedInterval.getSeconds());
    }

    private List<Changefeed> prepareChangefeeds(List<tech.ydb.yoj.databind.schema.Changefeed> changefeeds) {
        var changefeedNames = new HashSet<>();
        for (var changefeed : changefeeds) {
            String name = changefeed.name();
            if (name.isBlank()) {
                throw new IllegalArgumentException(
                        format("changefeed defined for %s has no name", getType()));
            }
            if (!changefeedNames.add(name)) {
                throw new IllegalArgumentException(
                        format("changefeed with name \"%s\" already defined for %s", name, getType())
                );
            }
        }
        return changefeeds.stream()
                .map(this::changefeedFromAnnotation)
                .toList();
    }

    private static Stream<JavaField> recurseFields(Collection<JavaField> fields) {
        return fields == null
                ? Stream.empty()
                : Stream.concat(fields.stream(), fields.stream().flatMap(f -> recurseFields(f.fields)));
    }

    private static List<GlobalIndex> collectIndexes(Class<?> type) {
        return List.of(type.getAnnotationsByType(GlobalIndex.class));
    }

    private static TTL extractTtlModifier(Class<?> type) {
        return type.getAnnotation(TTL.class);
    }

    private static List<tech.ydb.yoj.databind.schema.Changefeed> collectChangefeeds(Class<?> type) {
        return List.of(type.getAnnotationsByType(tech.ydb.yoj.databind.schema.Changefeed.class));
    }

    private JavaField newRootJavaField(@NonNull ReflectField field) {
        return new JavaField(field, null, this::isFlattenable);
    }

    private JavaField newRootJavaField(@NonNull JavaField javaField) {
        return new JavaField(javaField, null);
    }

    private Changefeed changefeedFromAnnotation(@NonNull tech.ydb.yoj.databind.schema.Changefeed changefeed) {
        var retentionPeriod = Duration.parse(changefeed.retentionPeriod());
        Preconditions.checkArgument(!(retentionPeriod.isNegative() || retentionPeriod.isZero()),
                "RetentionPeriod value defined for %s must be positive", getType());
        return new Changefeed(
                changefeed.name(),
                changefeed.mode(),
                changefeed.format(),
                changefeed.virtualTimestamps(),
                retentionPeriod,
                changefeed.initialScan(),
                Set.of(changefeed.consumers())
        );
    }

    /**
     * @param field {@link FieldValueType#isComposite() composite} field
     * @return {@code true} if the composite field can be flattened to a single field; {@code false otherwise}
     */
    protected boolean isFlattenable(ReflectField field) {
        return false;
    }

    public final Class<T> getType() {
        return schemaKey.clazz();
    }

    public final NamingStrategy getNamingStrategy() {
        return schemaKey.namingStrategy();
    }

    /**
     * DEPRECATED: old method, use correct instance of {@link TableDescriptor}
     * Returns the name of the table for data binding.
     * <p>
     * If the {@link Table} annotation is present, the field {@code name} should be used to
     * specify the table name.
     *
     * @return the table name for data binding
     */
    @Deprecated
    public final String getName() {
        return staticName;
    }

    public final List<JavaField> flattenFields() {
        return flattenedFieldStream().collect(toList());
    }

    public final List<String> flattenFieldNames() {
        return flattenedFieldStream().map(JavaField::getName).collect(toList());
    }

    private Stream<JavaField> flattenedFieldStream() {
        return fields.stream().flatMap(JavaField::flatten);
    }

    public final Map<String, Object> flatten(T t) {
        Map<String, Object> res = new LinkedHashMap<>();
        fields.forEach(f -> f.collectTo(t, res));
        return res;
    }

    public final Map<String, Object> flattenOneField(String fieldPath, Object fieldValue) {
        Map<String, Object> res = new LinkedHashMap<>();
        getField(fieldPath).collectValueTo(fieldValue, res);
        return res;
    }

    public final List<JavaFieldValue> flattenToList(T t) {
        return fields.stream()
                .flatMap(f -> f.flattenWithValue(t))
                .collect(toList());
    }

    /**
     * Creates a new object having the specified field values.
     *
     * @param cells field value map: <code>{@link JavaField#getName() field name} -> field value</code>
     * @return object with the specified field values
     * @throws ConstructionException could not construct object from {@code cells}
     */
    public final T newInstance(Map<String, Object> cells) throws ConstructionException {
        Object[] args = fields.stream().map(f -> f.newInstance(cells)).toArray();
        return safeNewInstance(reflectType.getConstructor(), args);
    }

    @SneakyThrows
    private static <T> T safeNewInstance(Constructor<T> ctor, Object[] args) throws ConstructionException {
        try {
            return ctor.newInstance(args);
        } catch (Exception e) {
            throw new ConstructionException(ctor, args, e);
        }
    }

    /**
     * @param path dot-separated field path, e.g. {@code vm.status} for the {@code status} field inside the
     *             {@code vm} field of the top-level entity
     * @return entity field
     * @throws IllegalArgumentException no such field exists
     */
    public final JavaField getField(String path) {
        return findField(path)
                .orElseThrow(() -> new IllegalArgumentException(format("No such field: \"%s\" in %s", path, getType())));
    }

    /**
     * @param path dot-separated field path, e.g. {@code vm.status} for the {@code status} field inside the
     *             {@code vm} field of the top-level entity
     * @return {@code Optional} representing the field found, if it exists;
     * an {@link Optional#empty() empty Optional} otherwise
     */
    public final Optional<JavaField> findField(String path) {
        return findField(path.split(Pattern.quote(PATH_DELIMITER)));
    }

    private Optional<JavaField> findField(String... pathComponents) {
        return fields.stream().map(f -> f.findField(asList(pathComponents))).filter(Objects::nonNull).findAny();
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(staticName);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Schema<?> other = (Schema<?>) o;
        return Objects.equals(staticName, other.staticName);
    }

    @Override
    public final String toString() {
        String schemaName = getClass().getSimpleName();
        if (schemaName.isEmpty()) {
            schemaName = getClass().getName();
        }

        return schemaName + " \"" + staticName + "\" [type=" + getType().getName() + "]";
    }

    private static final class DummyCustomValueSubField implements ReflectField {
        private final JavaField donor;

        private DummyCustomValueSubField(JavaField donor) {
            this.donor = donor;
        }

        @Override
        public String getName() {
            return donor.getName();
        }

        @Nullable
        @Override
        public Column getColumn() {
            return donor.getField().getColumn();
        }

        @Override
        public Type getGenericType() {
            return donor.getType();
        }

        @Override
        public Class<?> getType() {
            return donor.getRawType();
        }

        @Override
        public ReflectType<?> getReflectType() {
            return donor.getField().getReflectType();
        }

        @Override
        public Object getValue(Object containingObject) {
            Preconditions.checkArgument(donor.getRawType().equals(containingObject.getClass()),
                    "Tried to get value for a custom-value subfield '%s' on an invalid type: expected %s, got %s",
                    donor.getPath(),
                    donor.getRawType(),
                    containingObject.getClass()
            );
            return containingObject;
        }

        @Override
        public Collection<ReflectField> getChildren() {
            return Set.of();
        }

        @Override
        public FieldValueType getValueType() {
            return donor.getValueType();
        }

        @Nullable
        @Override
        public CustomValueTypeInfo<?, ?> getCustomValueTypeInfo() {
            return donor.getCustomValueTypeInfo();
        }

        @Override
        public String toString() {
            return "DummyStringValueField[donor=" + donor + "]";
        }
    }

    public static final class JavaField {
        @Getter
        private final ReflectField field;
        @Getter
        private final JavaField parent;
        @Getter
        private final FieldValueType valueType;
        @Getter
        private final boolean flattenable;
        @Getter
        private String name;
        @Getter
        private String path;

        private final List<JavaField> fields;

        private JavaField(ReflectField field, JavaField parent, Predicate<ReflectField> isFlattenable) {
            this.field = field;
            this.parent = parent;
            this.flattenable = isFlattenable.test(field);
            this.path = parent == null ? field.getName() : parent.getPath() + PATH_DELIMITER + field.getName();
            this.valueType = field.getValueType();
            if (valueType.isComposite()) {
                this.fields = field.getChildren().stream()
                        .map(f -> new JavaField(f, this, isFlattenable))
                        .toList();

                if (flattenable && isFlat()) {
                    toFlatField().path = path;
                }
            } else {
                this.fields = null;
            }
        }

        private JavaField(JavaField javaField, JavaField parent) {
            this.field = javaField.field;
            this.parent = parent;
            this.flattenable = javaField.flattenable;
            this.name = javaField.name;
            this.path = javaField.path;
            this.valueType = javaField.valueType;
            this.fields = (javaField.fields == null)
                    ? null
                    : javaField.fields.stream().map(f -> new JavaField(f, this)).toList();
        }

        /**
         * Returns the DB column type name (which is strongly DB-specific).
         * <p>
         * If the {@link Column} annotation is present, the field {@code dbType} may be used to
         * specify the DB column type.
         *
         * @return the DB column type for data binding if specified, {@code null} otherwise
         * @see Column
         */
        public DbType getDbType() {
            Column annotation = field.getColumn();
            if (annotation != null) {
                return annotation.dbType();
            }
            return DbType.DEFAULT;
        }

        /**
         * Returns the DB column type presentation qualifier name.
         *
         * @return the DB column type presentation qualifier for data binding if specified,
         * {@code null} otherwise
         * @see Column
         */
        public String getDbTypeQualifier() {
            Column annotation = field.getColumn();
            if (annotation != null && !annotation.dbTypeQualifier().isEmpty()) {
                return annotation.dbTypeQualifier();
            }
            return null;
        }

        public Type getType() {
            return field.getGenericType();
        }

        public Class<?> getRawType() {
            return field.getType();
        }

        // FIXME: make this method non-public
        @Deprecated
        public void setName(String newName) {
            this.name = newName;
        }

        @Beta
        public String getRawPath() {
            return getRawSubPath(0);
        }

        @Beta
        public String getRawSubPath(int start) {
            List<String> components = new ArrayList<>();
            JavaField p = this;
            do {
                components.add(p.field.getName());
                p = p.parent;
            } while (p != null);

            return components.size() > start
                    ? String.join(PATH_DELIMITER, Lists.reverse(components.subList(0, components.size() - start)))
                    : "";
        }

        public List<JavaField> getChildren() {
            return fields == null ? List.of() : List.copyOf(fields);
        }

        public Stream<JavaField> flatten() {
            return isSimple() ? Stream.of(this) : fields.stream().flatMap(JavaField::flatten);
        }

        public Stream<JavaFieldValue> flattenWithValue(Object o) {
            Object value = field.getValue(o);
            return isSimple()
                    ? Stream.of(new JavaFieldValue(this, value))
                    : fields.stream().flatMap(f -> f.flattenWithValue(value));
        }

        private void collectTo(Object o, Map<String, Object> res) {
            Object v = field.getValue(o);
            if (v != null) {
                collectValueTo(v, res);
            }
        }

        public void collectValueTo(Object v, Map<String, Object> res) {
            if (isSimple()) {
                res.put(name, v);
            } else {
                fields.forEach(f -> f.collectTo(v, res));
            }
        }

        /**
         * @return {@code true} if this is a simple (not composite) value; {@code false} otherwise
         */
        public boolean isSimple() {
            return fields == null;
        }

        /**
         * @return {@code true} if this field maps to a single database field, even if it is technically a composite
         * value;<br>
         * {@code false} otherwise
         * @see #isSimple()
         */
        public boolean isFlat() {
            return getSimpleFieldCardinality(this) == 1;
        }

        /**
         * Determining that a java field is mapped in more than one database field.
         *
         * @return {@code 0} if java field does not map in the database fields, {@code 1} maps to single database field
         * and more than {@code 1} if java field maps to more than one database field.
         */
        private static int getSimpleFieldCardinality(JavaField javaField) {
            if (javaField.isSimple()) {
                return 1;
            }

            boolean hasSimpleField = false;
            for (var field : javaField.fields) {
                switch (getSimpleFieldCardinality(field)) {
                    case 0:
                        break;

                    case 1:
                        if (hasSimpleField) {
                            return 2;
                        }

                        hasSimpleField = true;
                        break;

                    default:
                        return 2;
                }
            }

            return hasSimpleField ? 1 : 0;
        }

        /**
         * @return Java type of the lowest-level simple field, if this field {@link #isFlat() is flat}
         * @throws IllegalStateException field is not flat
         * @see #isFlat()
         * @see #toFlatField()
         */
        public Type getFlatFieldType() {
            return toFlatField().getType();
        }

        /**
         * @return single lowest-level simple field, if this field {@link #isFlat() is flat}
         * @throws IllegalStateException field is not flat
         * @see #isFlat()
         */
        public JavaField toFlatField() {
            try {
                return flatten().collect(onlyElement());
            } catch (IllegalArgumentException | NoSuchElementException e) {
                throw new IllegalStateException(format("Not a flat field: \"%s\"", path));
            }
        }

        @SneakyThrows
        private Object newInstance(Map<String, Object> cells) {
            if (isSimple()) {
                return cells.get(name);
            } else {
                Object[] args = fields.stream().map(f -> f.newInstance(cells)).toArray();
                if (Stream.of(args).allMatch(Objects::isNull)) {
                    return null;
                }
                return safeNewInstance(field.getReflectType().getConstructor(), args);
            }
        }

        private JavaField findField(List<String> path) {
            if (path.isEmpty()) {
                return null;
            }
            if (!field.getName().equals(path.get(0))) {
                return null;
            }
            if (path.size() == 1) {
                return this;
            }
            return fields == null ? null : fields.stream()
                    .map(f -> f.findField(path.subList(1, path.size())))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElse(null);
        }

        /**
         * @return information about custom value type for the schema field or its {@link #getRawType() class}
         * The {@link tech.ydb.yoj.databind.CustomValueType @CustomValueType} experimental annotation
         * specifies custom value conversion logic between Java field values and database column values.
         */
        @Nullable
        @SuppressWarnings("unchecked")
        @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
        public <J, C extends Comparable<? super C>> CustomValueTypeInfo<J, C> getCustomValueTypeInfo() {
            return (CustomValueTypeInfo<J, C>) field.getCustomValueTypeInfo();
        }

        @Override
        public String toString() {
            return getType().getTypeName() + " " + field.getName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JavaField other = (JavaField) o;
            return getType().getTypeName().equals(other.getType().getTypeName())
                    && valueType.name().equals(other.valueType.name())
                    && name.equals(other.name)
                    && path.equals(other.path)
                    && Objects.equals(fields, other.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getType().getTypeName(), valueType.name(), name, path, fields);
        }

        @NonNull
        public <T> JavaField forSchema(@NonNull Schema<T> dstSchema,
                                       @NonNull UnaryOperator<String> pathTransformer) {
            return dstSchema.getField(pathTransformer.apply(path));
        }
    }

    @Value
    public static class JavaFieldValue {
        @NonNull
        JavaField field;

        @Nullable
        Object value;

        public String getFieldPath() {
            return field.getPath();
        }

        public String getFieldName() {
            return field.getName();
        }

        public Type getFieldType() {
            return field.getType();
        }

        public FieldValueType getFieldValueType() {
            return field.getValueType();
        }
    }

    @Value
    @AllArgsConstructor
    public static class Index {
        public Index(@NonNull String indexName, @NonNull List<String> fieldNames) {
            this(indexName, fieldNames, false);
        }

        @NonNull
        String indexName;

        @With
        @NonNull
        List<String> fieldNames;

        boolean unique;
    }

    @Value
    public static class TtlModifier {
        @NonNull
        String fieldName;

        int interval;
    }

    @Value
    public static class Changefeed {
        @NonNull
        String name;

        @NonNull
        tech.ydb.yoj.databind.schema.Changefeed.Mode mode;

        @NonNull
        tech.ydb.yoj.databind.schema.Changefeed.Format format;

        boolean virtualTimestamps;

        @NonNull
        Duration retentionPeriod;

        boolean initialScan;

        @NonNull
        Set<String> consumers;
    }
}
