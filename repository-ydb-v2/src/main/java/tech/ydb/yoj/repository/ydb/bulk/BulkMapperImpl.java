package tech.ydb.yoj.repository.ydb.bulk;

import com.google.protobuf.NullValue;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.HashMap;
import java.util.Map;

public final class BulkMapperImpl<E extends Entity<E>> implements BulkMapper<E> {
    private final EntitySchema<E> srcSchema;

    public BulkMapperImpl(EntitySchema<E> srcSchema) {
        this.srcSchema = srcSchema;
    }

    @Override
    public String getTableName(String tableSpace) {
        return tableSpace + srcSchema.getName();
    }

    @Override
    public Map<String, ValueProtos.TypedValue> map(E entity) {
        var idComponents = srcSchema.flatten(entity);

        var result = new HashMap<String, ValueProtos.TypedValue>();
        for (var idField : srcSchema.flattenFields()) {
            var idFieldValue = idComponents.get(idField.getName());
            result.put(idField.getName(), toTypedValue(new Schema.JavaFieldValue(idField, idFieldValue), true));
        }

        return result;
    }

    protected ValueProtos.TypedValue toTypedValue(Schema.JavaFieldValue value, boolean optional) {
        YqlType type = YqlType.of(value.getField());
        return ValueProtos.TypedValue.newBuilder()
                .setType(getYqlType(type, optional))
                .setValue(getYqlValue(type, value.getValue()))
                .build();
    }

    protected ValueProtos.Type.Builder getYqlType(YqlType yqlType, boolean optional) {
        ValueProtos.Type.Builder ttype = yqlType.getYqlTypeBuilder();
        return !optional
                ? ttype
                : ValueProtos.Type.newBuilder().setOptionalType(ValueProtos.OptionalType.newBuilder().setItem(ttype));

    }

    protected ValueProtos.Value.Builder getYqlValue(YqlType type, Object value) {
        return value == null
                ? ValueProtos.Value.newBuilder().setNullFlagValue(NullValue.NULL_VALUE)
                : type.toYql(value);
    }
}
