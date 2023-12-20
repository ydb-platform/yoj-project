package tech.ydb.yoj.repository.ydb.bulk;

import tech.ydb.proto.ValueProtos;

import java.util.Map;

public interface BulkMapper<E> {
    String getTableName(String tableSpace);

    Map<String, ValueProtos.TypedValue> map(E entity);
}
