package tech.ydb.yoj.repository.ydb.spliterator.queue;

import javax.annotation.Nullable;

public interface YojSpliteratorQueue<V> {
    // (user thread) Get values from grpc-stream. Could be called only from one thread because of volatile closed variable
    @Nullable
    V poll();

    // (user thread) Could be called only from one thread with poll() because of volatile closed variable
    void close();
}
