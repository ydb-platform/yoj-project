package tech.ydb.yoj.repository.ydb.spliterator;

import java.util.Spliterator;

public interface ClosableSpliterator<V> extends Spliterator<V> {
    void close();
}
