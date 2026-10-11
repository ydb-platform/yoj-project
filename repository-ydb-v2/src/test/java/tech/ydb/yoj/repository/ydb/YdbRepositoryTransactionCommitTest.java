package tech.ydb.yoj.repository.ydb;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class YdbRepositoryTransactionCommitTest {
    @Parameters(name = "{0}")
    public static Object[][] data() {
        return new Object[][]{
                {StatusCode.ABORTED, OptimisticLockException.class},
                {StatusCode.BAD_SESSION, BadSessionException.class}
        };
    }

    @Parameter(0)
    public StatusCode statusCode;
    @Parameter(1)
    public Class<? extends RuntimeException> exceptionType;

    private final Session session = mock(Session.class);
    private final SessionManager sessionManager = mock(SessionManager.class);
    private final YdbSchemaOperations schemaOperations = mock(YdbSchemaOperations.class);
    private final TestYdbRepository testYdbRepository = mock(TestYdbRepository.class);

    @Before
    public void setUp() {
        when(testYdbRepository.getSessionManager()).thenReturn(sessionManager);
        when(testYdbRepository.getSchemaOperations()).thenReturn(schemaOperations);
        when(sessionManager.getSession()).thenReturn(session);
    }

    @Test
    public void invalidatedTransactionFailsWithoutInvokingDeferredCallback() {
        when(testYdbRepository.startTransaction(any(TxOptions.class))).thenCallRealMethod();
        var txManager = new StdTxManager(testYdbRepository).immediateWrites().withMaxRetries(0);
        Runnable callback = mock(Runnable.class);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Result.success(new DataQueryResult(
                        YdbTable.ExecuteQueryResult.newBuilder()
                                .setTxMeta(YdbTable.TransactionMeta.newBuilder().setId("tx-1"))
                                .build()
                ))),
                CompletableFuture.completedFuture(Result.<DataQueryResult>fail(Status.of(statusCode)))
        );

        assertThatIllegalStateException().isThrownBy(() -> txManager.tx(() -> {
            Tx tx = Tx.Current.get();
            tx.defer(callback);
            var complexes = tx.getRepositoryTransaction().table(Complex.class);
            complexes.save(new Complex(new Complex.Id(1, 1L, "c", Complex.Status.OK)));
            // Swallow the query error so TxManager attempts to commit the invalidated transaction.
            assertThatExceptionOfType(exceptionType)
                    .isThrownBy(() -> complexes.find(new Complex.Id(2, 2L, "c", Complex.Status.OK)));
        })).withMessageContaining("Transaction was invalidated");

        verify(callback, never()).run();
        verify(session, never()).commitTransaction(any(), any(CommitTxSettings.class));
        // YDB has already rolled back the transaction, so no rollback request must be sent (it would fail with NOT_FOUND)...
        verify(session, never()).rollbackTransaction(any(), any(RollbackTxSettings.class));
        // ...but the session must still be released
        verify(session).close();
    }

    @Test
    public void invalidatedTransactionDoesNotSendPendingWrites() {
        var tx = new TestYdbRepository.TestYdbRepositoryTransaction(
                testYdbRepository,
                TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE).withImmediateWrites(false)
        );
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Result.fail(Status.of(statusCode)))
        );

        tx.complexes().save(new Complex(new Complex.Id(1, 1L, "c", Complex.Status.OK)));
        verify(session, never()).executeDataQuery(any(), any(), any(), any());
        assertThatExceptionOfType(exceptionType)
                .isThrownBy(() -> tx.complexes().find(new Complex.Id(2, 2L, "c", Complex.Status.OK)));

        assertThatIllegalStateException().isThrownBy(tx::commit)
                .withMessageContaining("Transaction was invalidated");

        // Only the failed read was sent; commit must not flush the queued write.
        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
        verify(session).close();
    }
}
