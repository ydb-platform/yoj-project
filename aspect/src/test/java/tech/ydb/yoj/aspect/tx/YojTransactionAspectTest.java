package tech.ydb.yoj.aspect.tx;

import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;

import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.NonTx;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class YojTransactionAspectTest {
    private YojTransactionAspect aspect;
    private Repository mockRepository;
    private RepositoryTransaction mockRepositoryTransactions;

    @Before
    public void setup() {
        mockRepository = mock(Repository.class);
        mockRepositoryTransactions = mock(RepositoryTransaction.class);
        when(mockRepositoryTransactions.getTransactionLocal()).thenReturn(new TransactionLocal(
            TxOptions.create(IsolationLevel.ONLINE_CONSISTENT_READ_ONLY)));
        when(mockRepository.startTransaction((TxOptions) any())).thenReturn(mockRepositoryTransactions);
        aspect = new YojTransactionAspect(new StdTxManager(mockRepository));
    }

    @Test
    public void testSuccessfulTransaction() throws Throwable {
        ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
        when(pjp.proceed()).thenThrow(noRollbackExceptionHolder());
        YojTransactional transactional = this.getClass()
            .getDeclaredMethod("noRollbackExceptionHolder")
            .getDeclaredAnnotation(YojTransactional.class);
        assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, transactional)).isInstanceOf(ArithmeticException.class);
        verify(mockRepositoryTransactions, atLeast(1)).commit();
        verify(mockRepositoryTransactions, never()).rollback();
    }

    @Test
    public void testRetryableCanNotBeCommited() throws Throwable {
        ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
        when(pjp.proceed()).thenThrow(yojTransactionalHolder());
        YojTransactional transactional = this.getClass()
            .getDeclaredMethod("yojTransactionalHolder")
            .getDeclaredAnnotation(YojTransactional.class);
        assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, transactional)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRetryableExceptionIsPassedToTxManagerAsIs() throws Throwable {
        RetryableException original = new TestRetriableException("transient failure");
        ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
        when(pjp.proceed()).thenThrow(original);

        List<RetryableException> seenByTxManager = new ArrayList<>();
        YojTransactionAspect aspect = new YojTransactionAspect(new StdTxManager(mockRepository).withCustomRetries(e -> {
            seenByTxManager.add(e);
            return RetryPolicy.retryImmediately();
        }));
        YojTransactional transactional = this.getClass()
            .getDeclaredMethod("retryableHolder")
            .getDeclaredAnnotation(YojTransactional.class);

        assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, transactional))
            .isInstanceOf(UnavailableException.class)
            .satisfies(e -> assertThat(e.getCause()).isSameAs(original));
        // The original exception (and thus its retry policy) must be visible to the TxManager
        assertThat(seenByTxManager).hasSize(2).allMatch(e -> e == original);
        verify(mockRepositoryTransactions, times(3)).rollback();
        verify(mockRepositoryTransactions, never()).commit();
    }

    @YojTransactional(name = "retryable", maxRetries = 2)
    public void retryableHolder() {
    }

    @Test
    public void testNonTxRejectsTransactionsBeforeProceeding() throws Throwable {
        ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
        YojTransactional readWrite = getClass().getDeclaredMethod("noRollbackExceptionHolder")
                .getDeclaredAnnotation(YojTransactional.class);
        YojTransactional readOnly = getClass().getDeclaredMethod("readOnlyHolder")
                .getDeclaredAnnotation(YojTransactional.class);

        NonTx.nonTx(() -> assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, readWrite))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nonTx"));
        NonTx.nonTx(() -> assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, readOnly))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nonTx"));

        verifyNoInteractions(mockRepository);
        verify(pjp, never()).proceed();
    }

    @Test
    public void testTransactionsRejectNonTxBeforeRunningBody() throws Throwable {
        Runnable body = mock(Runnable.class);
        ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
        when(pjp.proceed()).thenAnswer(invocation -> {
            NonTx.nonTx(body);
            return null;
        });
        YojTransactional readWrite = getClass().getDeclaredMethod("noRollbackExceptionHolder")
                .getDeclaredAnnotation(YojTransactional.class);
        YojTransactional readOnly = getClass().getDeclaredMethod("readOnlyHolder")
                .getDeclaredAnnotation(YojTransactional.class);

        assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, readWrite))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("nonTx cannot be entered inside a transaction");
        assertThatThrownBy(() -> aspect.doInMethodTransaction(pjp, readOnly))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("nonTx cannot be entered inside a transaction");

        verifyNoInteractions(body);
        verify(pjp, times(2)).proceed();
        verify(mockRepositoryTransactions, times(2)).rollback();
        verify(mockRepositoryTransactions, never()).commit();
    }

    @YojTransactional(name = "readOnly", readOnly = true, isolation = IsolationLevel.SNAPSHOT_READ_ONLY)
    public void readOnlyHolder() {
    }

    @YojTransactional(name = "noRollbackFor", noRollbackFor = ArithmeticException.class)
    public Exception noRollbackExceptionHolder() {
        return new ArithmeticException();
    }

    @YojTransactional(name = "illegalStateException", noRollbackFor = TestRetriableException.class)
    public Exception yojTransactionalHolder() {
        return new TestRetriableException("message") {};
    }

    static class TestRetriableException extends RetryableException{
        protected TestRetriableException(String message) {
            super(message);
        }
    }
}
