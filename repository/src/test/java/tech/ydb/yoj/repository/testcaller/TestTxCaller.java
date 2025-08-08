package tech.ydb.yoj.repository.testcaller;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxNameGenerator;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.testcaller.TestDbTxCaller;

import javax.annotation.Nullable;
import java.util.Set;

import static lombok.AccessLevel.PRIVATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Part of {@link tech.ydb.yoj.repository.db.StdTxManagerTest}.
 * This is a copy of {@link TestDbTxCaller}
 * for testing calls from different packages.
 */
@With
@RequiredArgsConstructor(access = PRIVATE)
public class TestTxCaller {
    @Nullable
    private final String explicitName;

    @NonNull
    private final Set<String> skipCallerPackages;

    @NonNull
    private final TxNameGenerator txNameGenerator;

    public TestTxCaller(@Nullable String explicitName) {
        this(explicitName, Set.of(), explicitName == null ? TxNameGenerator.SHORT : TxNameGenerator.NONE);
    }

    public String getTxName() {
        var repo = mock(Repository.class);
        var rt = mock(RepositoryTransaction.class);
        when(rt.getTransactionLocal()).thenReturn(new TransactionLocal(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)));
        when(repo.startTransaction(any(TxOptions.class))).thenReturn(rt);

        var tx = new StdTxManager(repo)
                .withSkipCallerPackages(skipCallerPackages)
                .withTxNameGenerator(txNameGenerator);
        if (explicitName != null) {
            tx = tx.withName(explicitName);
        }
        return tx.tx(() -> Tx.Current.get().getName());
    }
}
