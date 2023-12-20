package tech.ydb.yoj.repository.test.sample;

import lombok.experimental.Delegate;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxManager;

public class TestDbImpl<R extends Repository> implements TestDb {
    @Delegate
    private final TxManager txManagerImpl;

    public TestDbImpl(R repository) {
        txManagerImpl = new StdTxManager(repository);
    }

    @Delegate
    protected TestEntityOperations db() {
        return (TestEntityOperations) Tx.Current.get().getRepositoryTransaction();
    }
}
