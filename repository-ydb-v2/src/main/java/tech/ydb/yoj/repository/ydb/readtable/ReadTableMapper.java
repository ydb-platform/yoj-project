package tech.ydb.yoj.repository.ydb.readtable;

import tech.ydb.proto.ValueProtos;

import java.util.List;

public interface ReadTableMapper<ID, RESULT> {
    String getTableName(String tableSpace);

    List<ValueProtos.TypedValue> mapKey(ID id);

    List<String> getColumns();

    RESULT mapResult(List<ValueProtos.Column> columnList, ValueProtos.Value value);
}
