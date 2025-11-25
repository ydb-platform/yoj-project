package tech.ydb.yoj.aspect.tx;

import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;

import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.RetryableException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class YojTransactionAspectTest {
    private YojTransactionAspect aspect;
    private RepositoryTransaction mockRepositoryTransactions;

    @Before
    public void setup() {
        Repository mockRepository = mock(Repository.class);
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
