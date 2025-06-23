package tech.ydb.yoj.repository.ydb.readtable;

import com.google.protobuf.NullValue;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.statement.ResultSetReader;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InternalApi
public final class EntityIdKeyMapper<E extends Entity<E>, ID extends Entity.Id<E>, RESULT> implements ReadTableMapper<ID, RESULT> {
    private final TableDescriptor<E> tableDescriptor;
    private final EntitySchema<E> srcSchema;
    private final Schema<RESULT> dstSchema;
    private final ResultSetReader<RESULT> resultSetReader;

    public EntityIdKeyMapper(TableDescriptor<E> tableDescriptor, EntitySchema<E> srcSchema, Schema<RESULT> dstSchema) {
        this.tableDescriptor = tableDescriptor;
        this.srcSchema = srcSchema;
        this.dstSchema = dstSchema;
        this.resultSetReader = new ResultSetReader<>(dstSchema);
    }

    @Override
    public List<ValueProtos.TypedValue> mapKey(ID id) {
        EntityIdSchema<ID> idSchema = srcSchema.getIdSchema();
        Map<String, Object> idComponents = idSchema.flatten(id);

        List<ValueProtos.TypedValue> idAsList = new ArrayList<>();
        for (Schema.JavaField idField : idSchema.flattenFields()) {
            Object idFieldValue = idComponents.get(idField.getName());
            idAsList.add(toTypedValue(new Schema.JavaFieldValue(idField, idFieldValue), true));
        }

        return idAsList;
    }

    @Override
    public List<String> getColumns() {
        return dstSchema.flattenFieldNames();
    }

    @Override
    public String getTableName(String tableSpace) {
        return tableSpace + tableDescriptor.tableName();
    }

    @Override
    public RESULT mapResult(List<ValueProtos.Column> columnList, ValueProtos.Value value) {
        return resultSetReader.readResult(columnList, value);
    }

    private ValueProtos.TypedValue toTypedValue(Schema.JavaFieldValue value, boolean optional) {
        YqlType type = YqlType.of(value.getField());
        return ValueProtos.TypedValue.newBuilder()
                .setType(getYqlType(type, optional))
                .setValue(getYqlValue(type, value.getValue()))
                .build();
    }

    private ValueProtos.Type.Builder getYqlType(YqlType yqlType, boolean optional) {
        ValueProtos.Type.Builder ttype = yqlType.getYqlTypeBuilder();
        return !optional
                ? ttype
                : ValueProtos.Type.newBuilder().setOptionalType(ValueProtos.OptionalType.newBuilder().setItem(ttype));

    }

    private ValueProtos.Value.Builder getYqlValue(YqlType type, Object value) {
        return value == null
                ? ValueProtos.Value.newBuilder().setNullFlagValue(NullValue.NULL_VALUE)
                : type.toYql(value);
    }
}
