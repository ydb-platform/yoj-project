package tech.ydb.yoj.repository.testcaller;

import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.testcaller.TestDbTxCaller;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Part of {@link import tech.ydb.yoj.repository.db.StdTxManagerTest}.
 * This is a copy of {@link TestDbTxCaller}
 * for testing calls from different packages.
 */
@RequiredArgsConstructor
public class TestTxCaller {
    private final String explicitName;
    private final Set<String> skipCallerPackages;

    public TestTxCaller(String explicitName) {
        this(explicitName, Set.of());
    }

    public String getTxName() {
        var repo = mock(Repository.class);
        var rt = mock(RepositoryTransaction.class);
        when(rt.getTransactionLocal()).thenReturn(new TransactionLocal(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)));
        when(repo.startTransaction(any(TxOptions.class))).thenReturn(rt);

        var tx = new StdTxManager(repo).withSkipCallerPackages(skipCallerPackages);
        if (explicitName != null) {
            tx = tx.withName(explicitName);
        }
        return tx.tx(() -> Tx.Current.get().getName());
    }
}
