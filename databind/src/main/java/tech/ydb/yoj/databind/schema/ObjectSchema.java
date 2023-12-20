package tech.ydb.yoj.databind.schema;

import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry;
import tech.ydb.yoj.databind.schema.configuration.SchemaRegistry.SchemaKey;
import tech.ydb.yoj.databind.schema.reflect.Reflector;

public final class ObjectSchema<O> extends Schema<O> {
    private ObjectSchema(SchemaKey<O> key, Reflector reflector) {
        super(key, reflector);
    }

    public static <O> ObjectSchema<O> of(Class<O> type) {
        return of(SchemaRegistry.getDefault(), type);
    }

    public static <O> ObjectSchema<O> of(SchemaRegistry registry, Class<O> type) {
        return registry.getOrCreate(ObjectSchema.class, ObjectSchema::new, SchemaKey.of(type));
    }
}
