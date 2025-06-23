package tech.ydb.yoj.repository.ydb.bulk;

import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.InternalApi;

import java.util.Map;

@InternalApi
public interface BulkMapper<E> {
    String getTableName(String tableSpace);

    Map<String, ValueProtos.TypedValue> map(E entity);
}
