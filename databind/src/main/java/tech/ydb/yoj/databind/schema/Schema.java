package tech.ydb.yoj.databind.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;
import tech.ydb.yoj.databind.schema.reflect.ReflectType;
import tech.ydb.yoj.databind.schema.reflect.Reflector;
import tech.ydb.yoj.databind.schema.reflect.StdReflector;
import tech.ydb.yoj.util.lang.Types;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public abstract class Schema<T> {
    public static final String PATH_DELIMITER = ".";

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

    private final Class<T> type;
    private final NamingStrategy namingStrategy;

    @Deprecated
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
        this.type = key.clazz();
        this.namingStrategy = key.namingStrategy();

        this.reflectType = reflector.reflectRootType(type);

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
        this(schema.getField(subSchemaFieldPath), schema.getNamingStrategy());
    }

    protected Schema(JavaField subSchemaField, @Nullable NamingStrategy parentNamingStrategy) {
        @SuppressWarnings("unchecked") ReflectType<T> rt = (ReflectType<T>) subSchemaField.field.getReflectType();

        this.reflectType = rt;
        this.type = rt.getRawType();
        this.namingStrategy = parentNamingStrategy == null ? SUBFIELD_SCHEMA_NAMING_STRATEGY : parentNamingStrategy;

        // This is a subfield, *NOT* an Entity, so it has no table name, no TTL, no indexes and no changefeeds
        // (And also, no useful naming strategy, because all field names have already been assigned by the moment you construct a subfield Schema!)
        this.staticName = "";
        this.ttlModifier = null;
        this.globalIndexes = List.of();
        this.changefeeds = List.of();

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
    }

    public final String getTypeName() {
        return type.getSimpleName();
    }

    private void validateFieldNames() {
        Map<String, JavaField> fieldNames = new HashMap<>();
        for (JavaField field : flattenFields()) {
            String fieldName = field.getName();
            JavaField existingField = fieldNames.putIfAbsent(fieldName, field);
            if (existingField != null) {
                throw new IllegalArgumentException("fields with same name \"%s\" detected: {%s} and {%s}"
                        .formatted(fieldName, field.getField(), existingField.getField()));
            }
        }
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
            List<JavaField> columns = new ArrayList<>(fieldPaths.length);
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
                columns.add(field);
            }
            outputIndexes.add(Index.builder()
                    .indexName(name)
                    .fields(columns)
                    .unique(index.type() == GlobalIndex.Type.UNIQUE)
                    .async(index.type() == GlobalIndex.Type.GLOBAL_ASYNC)
                    .build());
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
        return new TtlModifier(field, parsedInterval);
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
        List<Changefeed.Consumer> consumers = Arrays.stream(changefeed.consumers())
                .map(consumer -> new Changefeed.Consumer(
                        consumer.name(),
                        List.of(consumer.codecs()),
                        Instant.parse(consumer.readFrom()),
                        consumer.important()
                ))
                .toList();

        return new Changefeed(
                changefeed.name(),
                changefeed.mode(),
                changefeed.format(),
                changefeed.virtualTimestamps(),
                retentionPeriod,
                changefeed.initialScan(),
                consumers
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
        return type;
    }

    public final NamingStrategy getNamingStrategy() {
        return namingStrategy;
    }

    /**
     * @deprecated This method will be pulled down to {@code EntitySchema} in YOJ 3.0.0 or even earlier; and it might be removed in YOJ 3.x.
     * <br>YOJ end-users <strong>should never</strong> use this method themselves. To customize table name, just add the {@link Table} annotation to
     * an {@code Entity} and specify the desired name in the annotation's {@code name} field. To dynamically choose table name, use
     * the {@code BaseDb.table(TableDescriptor)} method inside your transaction.
     * <br>
     * This method always had somewhat unclear semantics (it was never specified what it returns for anything that's not an {@code EntitySchema})
     * and unnecessarily coupled the data-binding model ({@code Schema}s) to database concepts (tables, which have names, and implementation-defined
     * syntax for names and paths).
     *
     * @return this {@code Schema}'s "name", as determined by {@code NamingStrategy}. For {@code EntitySchema}, this will be the <em>table name</em>
     *         that's used if you don't obtain the table with an explicit {@code TableDescriptor}. Other instances of {@code Schema} are not
     *         guaranteed to return anything meaningful and/or useful from this method, and might return an empty {@code String}
     *         (but <em>not</em> {@code null}.)
     */
    @Deprecated(forRemoval = true)
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

    /**
     * Flattens a schema-conforming object into column representation: a {@code Map} of
     * <code>{@link JavaField#getName() simple field's column name} -> value</code>, with all simple column
     * values being of type specified in {@link FieldValueType} documentation for the corresponding
     * {@link JavaField#getValueType() field value type}. This operation is the opposite of {@link #newInstance(Map)}.
     * <p><strong>Warning:</strong> Do not make assumptions about the {@code Map} implementation returned.
     * Map entry order, presence of entries with {@code null} values and map (im)mutability are all considered
     * to be implementation details.
     *
     * @param t object to flatten
     * @return a {@code Map} of <code>{@link JavaField#getName() simple field's column name} -> value</code>
     *
     * @see #newInstance(Map)
     */
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
     * Creates a new object having the specified field values. The opposite operation is {@link #flatten(Object)}.
     *
     * @param cells field value map: <code>{@link JavaField#getName() simple field's column name} -> value</code>
     * @return object with the specified field values
     * @throws ConstructionException could not construct object from {@code cells}
     *
     * @see #flatten(Object)
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

    /**
     * @param indexName index name (the value of {@link GlobalIndex#name()} annotation property);
     *                  must not be {@code null}
     * @return index with the specified name
     * @throws IllegalArgumentException no index with the specified name exists
     */
    public Index getGlobalIndex(@NonNull String indexName) {
        return getGlobalIndexes().stream()
                .filter(idx -> indexName.equals(idx.getIndexName()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Index not found: '" + indexName + "'"));
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), getType(), getNamingStrategy());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Schema<?> otherSchema
                && otherSchema.getClass().equals(this.getClass())
                && otherSchema.getType().equals(this.getType())
                && otherSchema.getNamingStrategy().equals(this.getNamingStrategy());
    }

    @Override
    public final String toString() {
        String schemaClassName = Types.getShortTypeName(getClass());
        String staticTableName = staticName.isEmpty() ? "" : " \"" + staticName + "\"";

        return schemaClassName + staticTableName + " [type=" + getTypeName() + "]";
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

        /**
         * @deprecated Projections will be moved from the core YOJ API in 3.0.0 to an optional module.
         * The {@code getRawPath()} method is only used by projection logic, and will most likely be removed.
         * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
         */
        @Deprecated(forRemoval = true)
        public String getRawPath() {
            return getRawSubPath(0);
        }

        /**
         * @deprecated Projections will be moved from the core YOJ API in 3.0.0 to an optional module.
         * The {@code getRawSubPath(int)} method is only used by projection logic, and will most likely be removed.
         * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
         */
        @Deprecated(forRemoval = true)
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
         * @return the uppermost field that contains this flat field and is still {@link #isFlat() flat}; {@code this} if no such flat field exists
         * @throws IllegalStateException if this field is not {@link #isFlat() flat}
         */
        @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/pull/130")
        public JavaField getFlatRoot() {
            Preconditions.checkState(isFlat(), "Cannot get flat parent for a non-flat field");

            JavaField flatRoot = this;
            while (flatRoot.parent != null && flatRoot.parent.getChildren().size() == 1) {
                flatRoot = flatRoot.parent;
            }
            return flatRoot;
        }

        /**
         * @param condition the condition for matching the fields
         * @return the outermost flat child field that {@code this} field contains (including {@code this} itself!) that matches the {@code condition}
         * @throws IllegalStateException if this field is not {@link #isFlat() flat}
         */
        @Nullable
        @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/pull/130")
        public JavaField findFlatChild(@NonNull Predicate<JavaField> condition) {
            Preconditions.checkState(isFlat(), "Cannot get flat child for a non-flat field");

            JavaField current = this;
            while (current != null) {
                if (condition.test(current)) {
                    return current;
                }
                current = current.getChildren().isEmpty() ? null : current.getChildren().get(0);
            }
            return null;
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
    @Builder
    public static class Index {
        String indexName;

        @EqualsAndHashCode.Exclude
        List<JavaField> fields;

        @ToString.Exclude
        List<String> fieldNames;

        boolean unique;
        boolean async;

        @java.beans.ConstructorProperties({"indexName", "fields", "fieldNames", "unique", "async"})
        private Index(
                @NonNull String indexName,
                @NonNull List<JavaField> fields, @NonNull List<String> fieldNames,
                boolean unique,
                boolean async
        ) {
            this.indexName = indexName;

            Preconditions.checkArgument(!fields.isEmpty() && !fieldNames.isEmpty(), "Index must have at least 1 field");
            this.fields = fields;
            this.fieldNames = fieldNames;

            this.unique = unique;
            this.async = async;
        }

        public static class IndexBuilder {
            public IndexBuilder fields(List<JavaField> fields) {
                this.fields = List.copyOf(fields);
                return fieldNames(fields.stream().map(JavaField::getName).toList());
            }

            private IndexBuilder fieldNames(List<String> fieldNames) {
                this.fieldNames = fieldNames;
                return this;
            }
        }
    }

    @Value
    public static class TtlModifier {
        @NonNull
        JavaField field;

        @NonNull
        Duration interval;

        @NonNull
        public String getFieldName() {
            return field.getName();
        }

        public int getIntervalSeconds() {
            return Math.toIntExact(interval.getSeconds());
        }
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
        List<Consumer> consumers;

        @Value
        public static class Consumer {
            @NonNull
            String name;

            @NonNull
            List<tech.ydb.yoj.databind.schema.Changefeed.Consumer.Codec> codecs;

            @NonNull
            Instant readFrom;

            boolean important;
        }
    }

    private static final NamingStrategy SUBFIELD_SCHEMA_NAMING_STRATEGY = new NamingStrategy() {
        @Override
        public String getNameForClass(@NonNull Class<?> entityClass) {
            throw new UnsupportedOperationException("Schema.SUBFIELD_SCHEMA_NAMING_STRATEGY.getNameForClass() must never be called");
        }

        @Override
        public void assignFieldName(@NonNull JavaField javaField) {
            throw new UnsupportedOperationException("Schema.SUBFIELD_SCHEMA_NAMING_STRATEGY.assignFieldName() must never be called");
        }

        @Override
        public String toString() {
            return "Schema.SUBFIELD_SCHEMA_NAMING_STRATEGY";
        }
    };
}
