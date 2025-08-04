package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static tech.ydb.yoj.databind.FieldValueType.BOOLEAN;
import static tech.ydb.yoj.databind.FieldValueType.BYTE_ARRAY;
import static tech.ydb.yoj.databind.FieldValueType.ENUM;
import static tech.ydb.yoj.databind.FieldValueType.INTEGER;
import static tech.ydb.yoj.databind.FieldValueType.STRING;
import static tech.ydb.yoj.databind.FieldValueType.TIMESTAMP;
import static tech.ydb.yoj.databind.FieldValueType.UUID;
import static tech.ydb.yoj.databind.schema.naming.NamingStrategy.NAME_DELIMITER;

public final class EntityIdSchema<ID extends Entity.Id<?>> extends Schema<ID> implements Comparator<ID> {
    /**
     * {@link JavaField#getName() Name} and also {@link JavaField#getPath() path} of Entity's {@link Entity#getId() ID}
     * field. Use this constant if you need to reference ID field in a query, filter or order expression.<br>
     * If you need to check whether the specified field is a [part of] ID field, use the {@link #isIdField(JavaField)},
     * {@link #isIdFieldPath(String)} and {@link #isIdFieldName(String)} methods instead.
     *
     * @see #isIdField(JavaField)
     * @see #isIdFieldPath(String)
     * @see #isIdFieldName(String)
     */
    public static final String ID_FIELD_NAME = "id";
    private static final String ID_SUBFIELD_PATH_PREFIX = ID_FIELD_NAME + PATH_DELIMITER;
    private static final String ID_SUBFIELD_NAME_PREFIX = ID_FIELD_NAME + NAME_DELIMITER;

    public static final Comparator<Entity<?>> SORT_ENTITY_BY_ID = Comparator.comparing(
            Entity::getId, (a, b) -> EntityIdSchema.ofEntity(a.getType()).compare(a, b)
    );

    public static <T extends Entity<T>> Comparator<Entity.Id<T>> getIdComparator(Class<T> type) {
        return Comparator.comparing(
                id -> id, (a, b) -> EntityIdSchema.ofEntity(type).compare(a, b)
        );
    }

    private static final Type ENTITY_TYPE_PARAMETER = Entity.Id.class.getTypeParameters()[0];

    private static final Set<FieldValueType> ALLOWED_ID_FIELD_TYPES = Set.of(
            STRING, INTEGER, ENUM, BOOLEAN, TIMESTAMP, UUID, BYTE_ARRAY
    );

    private <E extends Entity<E>> EntityIdSchema(EntitySchema<E> entitySchema) {
        super(entitySchema, ID_FIELD_NAME);

        var flattenedFields = flattenFields();

        var idField = entitySchema.getField(ID_FIELD_NAME);
        if (idField.getValueType().isComposite()) {
            Preconditions.checkArgument(!flattenedFields.isEmpty(), "ID must have at least 1 field, but got none: %s", idField.getType());
        } else {
            Preconditions.checkArgument(
                    idField.getCustomValueTypeInfo() != null,
                    "ID must be either a composite with >= 1 field, or a compatible type annotated with @CustomValueType, but got: %s",
                    idField.getType()
            );
        }

        flattenedFields.stream()
                .filter(f -> !ALLOWED_ID_FIELD_TYPES.contains(FieldValueType.forSchemaField(f)))
                .findAny()
                .ifPresent(f -> {
                    throw new IllegalArgumentException(String.format(
                            "Leaf ID field \"[%s].%s\" <java=%s, db=%s> is none of the allowed types %s",
                            getType().getName(), f.getName(), f.getType(),
                            FieldValueType.forSchemaField(f), ALLOWED_ID_FIELD_TYPES));
                });
    }

    @Override
    protected boolean isFlattenable(ReflectField field) {
        return true;
    }

    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> ofEntity(Class<T> entityType) {
        return ofEntity(entityType, null);
    }

    /**
     * @param namingStrategy naming strategy with mandatory equals/hashCode.
     */
    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> ofEntity(
            Class<T> entityType, NamingStrategy namingStrategy) {
        return ofEntity(SchemaRegistry.getDefault(), entityType, namingStrategy);
    }

    /**
     * @param namingStrategy naming strategy with mandatory equals/hashCode.
     */
    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> ofEntity(
            SchemaRegistry registry, Class<T> entityType, NamingStrategy namingStrategy) {
        EntitySchema<T> entitySchema = EntitySchema.of(registry, entityType, namingStrategy);
        return from(entitySchema);
    }

    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> of(Class<ID> idType) {
        return of(idType, null);
    }

    /**
     * @param namingStrategy naming strategy with mandatory equals/hashCode.
     */
    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> of(
            Class<ID> idType, NamingStrategy namingStrategy) {
        return of(SchemaRegistry.getDefault(), idType, namingStrategy);
    }

    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> of(SchemaRegistry registry,
                                                                                       Class<ID> idType) {
        return of(registry, idType, null);
    }

    /**
     * @param namingStrategy naming strategy with mandatory equals/hashCode.
     */
    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> of(
            SchemaRegistry registry,
            Class<ID> idType, NamingStrategy namingStrategy) {
        @SuppressWarnings("unchecked") Class<T> entityType = (Class<T>) resolveEntityType(idType);
        EntitySchema<T> entitySchema = EntitySchema.of(registry, entityType, namingStrategy);
        return from(entitySchema);
    }

    public static <T extends Entity<T>, ID extends Entity.Id<T>> EntityIdSchema<ID> from(EntitySchema<T> entitySchema) {
        var key = SchemaKey.of(entitySchema.getType(), entitySchema.getNamingStrategy());
        return entitySchema.getRegistry().getOrCreate(EntityIdSchema.class, (k, r) -> new EntityIdSchema<>(entitySchema), key);
    }

    static Class<?> resolveEntityType(Type idType) {
        return TypeToken.of(idType).resolveType(ENTITY_TYPE_PARAMETER).getRawType();
    }

    public static boolean isIdField(@NonNull JavaField field) {
        return isIdFieldPath(field.getPath());
    }

    public static boolean isIdFieldPath(@NonNull String path) {
        return path.equals(ID_FIELD_NAME) || path.startsWith(ID_SUBFIELD_PATH_PREFIX);
    }

    public static boolean isIdFieldName(@NonNull String name) {
        return name.equals(ID_FIELD_NAME) || name.startsWith(ID_SUBFIELD_NAME_PREFIX);
    }

    @Override
    public int compare(@NonNull ID a, @NonNull ID b) {
        Map<String, Object> idA = flatten(a);
        Map<String, Object> idB = flatten(b);

        List<JavaField> flatFields = flattenFields();
        for (JavaField field : flatFields) {
            var fieldName = field.getName();

            @SuppressWarnings("unchecked")
            int res = compare(toComparable(idA.get(fieldName), field), toComparable(idB.get(fieldName), field));

            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    private static Comparable toComparable(@Nullable Object value, @NonNull JavaField field) {
        if (value == null) {
            return null;
        }

        value = CustomValueTypes.preconvert(field, value);
        if (value instanceof Enum) {
            return ((Enum) value).name();
        } else if (value instanceof Comparable) {
            // String, Instant, Boolean
            return (Comparable) value;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        throw new IllegalArgumentException("ID fields must implement Comparable");
    }

    private static <E extends Comparable<? super E>> int compare(@Nullable E a, @Nullable E b) {
        // nulls first
        if (a == null && b == null) {
            return 0;
        } else if (a == null /* && b != null */) {
            return -1;
        } else if (/* a != null && */ b == null) {
            return 1;
        } else /* if (a != null && b != null) */ {
            return a.compareTo(b);
        }
    }
}
