package tech.ydb.yoj.repository.db;

import org.junit.Test;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.cache.TransactionLog;

import static com.google.common.util.concurrent.Runnables.doNothing;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StrictTxManagerTest {
    private final Repository repository = mock(Repository.class);
    private final TransactionLocal transactionLocal = mock(TransactionLocal.class);
    private final TransactionLog transactionLog = new TransactionLog(TransactionLog.Level.DEBUG);
    private final RepositoryTransaction repositoryTransaction = mock(RepositoryTransaction.class);

    private TxManager getTxManager() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);
        return new StdTxManager(repository).withDryRun(true).failOnUnknownSeparateTx();
    }

    @Test(expected = IllegalStateException.class)
    public void testRwInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.tx(doNothing()));
    }

    @Test
    public void testSeparateRwInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.separate().tx(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testRoInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.readOnly().run(doNothing()));
    }

    @Test
    public void testSeparateRoInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.separate().readOnly().run(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testScanInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.scan().run(doNothing()));
    }

    @Test
    public void testSeparateScanInRwFailed() {
        TxManager txManager = getTxManager();

        txManager.tx(() -> txManager.separate().scan().run(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testRwInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.tx(doNothing()));
    }

    @Test
    public void testSeparateRwInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.separate().tx(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testScanInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.scan().run(doNothing()));
    }

    @Test
    public void testSeparateScanInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.separate().scan().run(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testRoInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.readOnly().run(doNothing()));
    }

    @Test
    public void testSeparateRoInRoFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.separate().readOnly().run(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testRwInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.readOnly().run(() -> txManager.tx(doNothing()));
    }

    @Test
    public void testSeparateRwInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.scan().run(() -> txManager.separate().tx(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testScanInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.scan().run(() -> txManager.scan().run(doNothing()));
    }

    @Test
    public void testSeparateScanInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.scan().run(() -> txManager.separate().scan().run(doNothing()));
    }

    @Test(expected = IllegalStateException.class)
    public void testRoInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.scan().run(() -> txManager.readOnly().run(doNothing()));
    }

    @Test
    public void testSeparateRoInScanFailed() {
        TxManager txManager = getTxManager();

        txManager.scan().run(() -> txManager.separate().readOnly().run(doNothing()));
    }
}
