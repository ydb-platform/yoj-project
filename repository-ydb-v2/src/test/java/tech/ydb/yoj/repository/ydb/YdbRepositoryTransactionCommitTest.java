package tech.ydb.yoj.repository.ydb;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class YdbRepositoryTransactionCommitTest {
    @Mock
    private Session session;
    @Mock
    private SessionManager sessionManager;
    @Mock
    private YdbSchemaOperations schemaOperations;
    @Mock
    private TestYdbRepository testYdbRepository;

    private AutoCloseable mockitoCloseable;

    @Before
    public void setUp() {
        mockitoCloseable = MockitoAnnotations.openMocks(this);

        when(testYdbRepository.getSessionManager()).thenReturn(sessionManager);
        when(testYdbRepository.getSchemaOperations()).thenReturn(schemaOperations);
        when(sessionManager.getSession()).thenReturn(session);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        if (mockitoCloseable != null) {
            mockitoCloseable.close();
        }
    }

    @Test
    public void commitAfterTransactionWasInvalidatedByServerFails() {
        var tx = new TestYdbRepository.TestYdbRepositoryTransaction(
                testYdbRepository,
                TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE).withImmediateWrites(true)
        );

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Result.success(new DataQueryResult(
                        YdbTable.ExecuteQueryResult.newBuilder()
                                .setTxMeta(YdbTable.TransactionMeta.newBuilder().setId("tx-1"))
                                .build()
                ))),
                CompletableFuture.completedFuture(Result.<DataQueryResult>fail(Status.of(StatusCode.ABORTED)))
        );

        tx.complexes().save(new Complex(new Complex.Id(1, 1L, "c", Complex.Status.OK)));
        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(() -> tx.complexes().find(new Complex.Id(2, 2L, "c", Complex.Status.OK)));

        // User code has swallowed the exception and tries to commit the transaction anyway
        assertThatIllegalStateException().isThrownBy(tx::commit);

        verify(session, never()).commitTransaction(any(), any(CommitTxSettings.class));
        verify(session).close();
    }
}
