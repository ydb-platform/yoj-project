package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;
import tech.ydb.yoj.databind.schema.reflect.Reflector;
import tech.ydb.yoj.util.lang.Annotations;

import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static lombok.AccessLevel.PACKAGE;

public final class EntitySchema<T extends Entity<T>> extends Schema<T> {
    private static final Type ENTITY_TYPE_PARAMETER = Entity.class.getTypeParameters()[0];

    @Getter(PACKAGE)
    private final SchemaRegistry registry;

    @Getter
    private final boolean useDescriptor;

    private final String staticName;

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
        checkIdField();

        this.registry = registry;

        var entityType = getType();
        var tableAnnotation = Annotations.find(tech.ydb.yoj.databind.schema.Table.class, entityType);

        this.useDescriptor = tableAnnotation != null && tableAnnotation.explicitDescriptor();
        this.staticName = getNamingStrategy().getNameForClass(entityType);
    }

    private static <T extends Entity<T>> SchemaKey<T> checkEntityType(SchemaKey<T> key) {
        Class<T> entityType = key.clazz();

        Preconditions.checkArgument(Entity.class.isAssignableFrom(entityType),
                "Entity type <%s> must implement <%s>", entityType.getTypeName(), Entity.class.getTypeName());

        Class<?> entityTypeFromEntityIface = resolveEntityTypeFromEntityIface(entityType);
        Preconditions.checkArgument(entityTypeFromEntityIface.equals(entityType),
                "Entity type <%s> must implement <%s> specified by the same type, but it is specified by <%s>",
                entityType.getTypeName(), Entity.class.getTypeName(), entityTypeFromEntityIface.getTypeName());

        return key;
    }

    private void checkIdField() {
        Class<T> entityType = getType();

        JavaField idField = findField(EntityIdSchema.ID_FIELD_NAME).orElse(null);
        Preconditions.checkArgument(idField != null, "Entity type <%s> does not contain a mandatory \"%s\" field",
                entityType.getTypeName(), EntityIdSchema.ID_FIELD_NAME);

        Type idFieldType = idField.getType();
        Preconditions.checkArgument(
                Entity.Id.class.isAssignableFrom(idField.getRawType()),
                "Entity ID type <%s> must implement <%s>", idFieldType.getTypeName(), Entity.Id.class.getTypeName()
        );

        Class<?> entityTypeFromIdType = EntityIdSchema.resolveEntityType(idFieldType);
        Preconditions.checkArgument(entityTypeFromIdType.equals(entityType),
                "ID field %s has a type <%s> that is not a valid ID type for entity of type <%s>",
                idField, idFieldType.getTypeName(), entityType.getTypeName()
        );
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

    public <V extends Table.View> ViewSchema<V> getViewSchema(Class<V> viewClass) {
        return ViewSchema.of(getRegistry(), viewClass, getNamingStrategy());
    }

    /**
     * Returns this {@code EntitySchema}'s <em>default table name</em>, as determined by its {@code NamingStrategy}.
     * This name will be used by {@link tech.ydb.yoj.repository.BaseDb#table(Class) BaseDb.table()} if you don't specify
     * an {@link tech.ydb.yoj.repository.BaseDb#table(TableDescriptor) explicit TableDescriptor}.
     *
     * @return this {@code EntitySchema}'s <em>default table name</em>
     * @throws IllegalStateException This entity is annotated as requiring an explicit {@code TableDescriptor}
     *                               (by {@link tech.ydb.yoj.databind.schema.Table @Table(explicitDescriptor=true)}),
     *                               and thus has no <em>default table name</em>
     */
    public String getName() {
        Preconditions.checkState(!useDescriptor,
                "getName() not supported for entity <%s>. Use BaseDb.table(TableDescriptor) to specify table name",
                getTypeName()
        );
        return staticName;
    }

    /**
     * @return a comparator for sorting entities by ID ascending (default YOJ sort order)
     */
    public Comparator<T> defaultOrder() {
        return Comparator.comparing(Entity::getId, getIdSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(staticName);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof EntitySchema<?> other && Objects.equals(staticName, other.staticName);
    }
}
