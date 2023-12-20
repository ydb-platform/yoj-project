package tech.ydb.yoj.repository.ydb.bulk;

import com.yandex.ydb.ValueProtos;

import java.util.Map;

public interface BulkMapper<E> {
    String getTableName(String tableSpace);

    Map<String, ValueProtos.TypedValue> map(E entity);
}
