package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.naming.NamingStrategy;
import tech.ydb.yoj.databind.schema.reflect.ReflectField;
import tech.ydb.yoj.databind.schema.reflect.Reflector;

public final class ViewSchema<VIEW extends Table.View> extends Schema<VIEW> {
    private ViewSchema(SchemaKey<VIEW> key, Reflector reflector) {
        super(key, reflector);
    }

    public static <VIEW extends Table.View> ViewSchema<VIEW> of(Class<VIEW> type) {
        return of(type, null);
    }

    public static <VIEW extends Table.View> ViewSchema<VIEW> of(Class<VIEW> type, NamingStrategy namingStrategy) {
        return of(SchemaRegistry.getDefault(), type, namingStrategy);
    }

    public static <VIEW extends Table.View> ViewSchema<VIEW> of(SchemaRegistry registry, Class<VIEW> type) {
        return of(registry, type, null);
    }

    public static <VIEW extends Table.View> ViewSchema<VIEW> of(SchemaRegistry registry,
                                                                Class<VIEW> type, NamingStrategy namingStrategy) {
        return registry.getOrCreate(ViewSchema.class, ViewSchema::new, SchemaKey.of(type, namingStrategy));
    }

    @Override
    protected boolean isFlattenable(ReflectField field) {
        return Entity.Id.class.isAssignableFrom(field.getType());
    }

    @Override
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/149")
    protected boolean isRequiredField(ReflectField field) {
        if (Entity.Id.class.isAssignableFrom(field.getType())) {
            return switch (EntityIdFieldNullability.get()) {
                case ALWAYS_NULL -> false;
                case USE_COLUMN_ANNOTATION -> super.isRequiredField(field);
                case ALWAYS_NOT_NULL -> true;
            };
        } else {
            return super.isRequiredField(field);
        }
    }
}
