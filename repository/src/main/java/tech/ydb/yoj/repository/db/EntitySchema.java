package tech.ydb.yoj.repository.db;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;
import tech.ydb.yoj.databind.schema.reflect.Reflector;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static lombok.AccessLevel.PACKAGE;

public final class EntitySchema<T extends Entity<T>> extends Schema<T> {
    private static final Type ENTITY_TYPE_PARAMETER = Entity.class.getTypeParameters()[0];

    @Getter(PACKAGE)
    private final SchemaRegistry registry;

    public static <T extends Entity<T>> EntitySchema<T> of(Class<T> type) {
        return of(type, null);
    }

    public static <T extends Entity<T>> EntitySchema<T> of(Class<T> type, NamingStrategy namingStrategy) {
        return of(SchemaRegistry.getDefault(), type, namingStrategy);
    }

    public static <T extends Entity<T>> EntitySchema<T> of(SchemaRegistry registry, Class<T> type) {
        return of(registry, type, null);
    }

    /**
     * @param namingStrategy naming strategy with mandatory equals/hashCode.
     */
    public static <T extends Entity<T>> EntitySchema<T> of(SchemaRegistry registry, Class<T> type, NamingStrategy namingStrategy) {
        return of(registry, SchemaKey.of(type, namingStrategy));
    }

    public static <T extends Entity<T>> EntitySchema<T> of(SchemaRegistry registry, SchemaKey<T> key) {
        return registry.getOrCreate(EntitySchema.class, (k, r) -> new EntitySchema<>(k, r, registry), key);
    }

    private EntitySchema(SchemaKey<T> key, Reflector reflector, SchemaRegistry registry) {
        super(checkEntityType(key), reflector);
        this.registry = registry;
    }

    private static <T extends Entity<T>> SchemaKey<T> checkEntityType(SchemaKey<T> key) {
        Class<T> entityType = key.clazz();

        if (!Entity.class.isAssignableFrom(entityType)) {
            throw new IllegalArgumentException(format(
                    "Entity type [%s] must implement [%s]", entityType.getName(), Entity.class));
        }

        Class<?> entityTypeFromEntityIface = resolveEntityTypeFromEntityIface(entityType);
        if (!entityTypeFromEntityIface.equals(entityType)) {
            throw new IllegalArgumentException(format(
                    "Entity type [%s] must implement [%s] specified by the same type, but it is specified by [%s]",
                    entityType.getName(), Entity.class, entityTypeFromEntityIface.getName()));
        }

        Class<?> idFieldType;
        try {
            idFieldType = entityType.getDeclaredField(EntityIdSchema.ID_FIELD_NAME).getType();
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format(
                    "Entity type [%s] does not contain a mandatory \"%s\" field",
                    entityType.getName(), EntityIdSchema.ID_FIELD_NAME));
        }

        if (!Entity.Id.class.isAssignableFrom(idFieldType)) {
            throw new IllegalArgumentException(format(
                    "Entity ID type [%s] must implement [%s]", idFieldType.getName(), Entity.Id.class));
        }

        Class<?> entityTypeFromIdType = EntityIdSchema.resolveEntityType(idFieldType);
        if (!entityTypeFromIdType.equals(entityType)) {
            throw new IllegalArgumentException(format(
                    "An identifier field \"%s\" has a type [%s] that is not an identifier type for an entity of type [%s]",
                    EntityIdSchema.ID_FIELD_NAME, idFieldType.getName(), entityType.getName()));
        }

        return key;
    }

    static Class<?> resolveEntityTypeFromEntityIface(Class<?> entityType) {
        return TypeToken.of(entityType).resolveType(ENTITY_TYPE_PARAMETER).getRawType();
    }

    @Override
    protected boolean isFlattenable(ReflectField field) {
        return Entity.Id.class.isAssignableFrom(field.getType());
    }

    public <ID extends Entity.Id<T>> EntityIdSchema<ID> getIdSchema() {
        return EntityIdSchema.from(this);
    }

    public List<JavaField> flattenId() {
        return getIdSchema().flattenFields();
    }

    public Map<String, Object> flattenId(Entity.Id<T> idValue) {
        return getIdSchema().flatten(idValue);
    }
}
