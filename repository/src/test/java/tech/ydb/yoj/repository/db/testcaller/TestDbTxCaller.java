package tech.ydb.yoj.repository.db.testcaller;

import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxNameGenerator;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.testcaller.TestTxCaller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Part of {@link tech.ydb.yoj.repository.db.StdTxManagerTest}.
 * This is a copy of {@link TestTxCaller}
 * for testing calls from different packages.
 */
@RequiredArgsConstructor
public class TestDbTxCaller {
    private final TxNameGenerator txNameGenerator;

    public String getTxName() {
        var repo = mock(Repository.class);
        var rt = mock(RepositoryTransaction.class);
        when(rt.getTransactionLocal()).thenReturn(new TransactionLocal(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)));
        when(repo.startTransaction(any(TxOptions.class))).thenReturn(rt);

        var txManager = new StdTxManager(repo).withTxNameGenerator(txNameGenerator);
        return txManager.tx(() -> Tx.Current.get().getName());
    }
}
