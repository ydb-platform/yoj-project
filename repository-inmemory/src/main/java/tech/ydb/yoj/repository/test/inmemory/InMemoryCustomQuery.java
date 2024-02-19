package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Tx;

import java.util.List;

public interface InMemoryCustomQuery<T> extends Tx.CustomQuery<T> {
    Stream<T> query(PublicInMemoryStorage storage, InMemoryTxLockWatcher watcher);
}
