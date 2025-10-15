package tech.ydb.yoj.repository.db;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;
import org.mockito.Mockito;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.cache.TransactionLog;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.testcaller.TestDbTxCaller;
import tech.ydb.yoj.repository.testcaller.TestTxCaller;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StdTxManagerTest {
    private final Repository repository = mock(Repository.class);
    private final TransactionLocal transactionLocal = mock(TransactionLocal.class);
    private final TransactionLog transactionLog = new TransactionLog(TransactionLog.Level.DEBUG);
    private final RepositoryTransaction repositoryTransaction = mock(RepositoryTransaction.class);

    @Test
    public void testDbChildPackage_FromStackWalker_Auto() {
        var name = new TestDbTxCaller(new TxNameGenerator.Default(Set.of())).getTxName();
        assertThat(name).isEqualTo("TesDbTxCal#getTxNam");
    }

    @Test
    public void testDbChildPackage_FromStackWalker_User() {
        var name = new TestDbTxCaller(new TxNameGenerator.Simple("qq")).getTxName();
        assertThat(name).isEqualTo("qq");
    }

    @Test
    public void testNotDbChildPackage_FromStackWalker_Auto() {
        var name = new TestTxCaller(new TxNameGenerator.Default(Set.of())).getTxName();
        assertThat(name).isEqualTo("TesTxCal#getTxNam");
    }

    @Test
    public void testNotDbChildPackage_FromStackWalker_Auto_SkipCallerPackage() {
        var name = new TestTxCaller(new TxNameGenerator.Default(Set.of(TestTxCaller.class.getPackageName())))
                .getTxName();
        assertThat(name).isEqualTo("StdTxManTes#testNotDbChiPac_FroStaWal_Aut_SkiCalPac");
    }

    @Test
    public void testNotDbPackage_Same() {
        var nameOld = new TestTxCaller(new TxNameGenerator.Default(Set.of())).getTxName();
        var nameNew = new TestTxCaller(new TxNameGenerator.Default(Set.of())).getTxName();
        assertThat(nameNew).isEqualTo(nameOld);
    }

    @Test
    public void testNotDbPackage_FromStackWalker_User() {
        var name = new TestTxCaller(new TxNameGenerator.Simple("omg")).getTxName();
        assertThat(name).isEqualTo("omg");
    }

    // explicitly TxNameGenerator.LONG
    @Test
    public void testDbChildPackage_FromStackWalker_Auto_Long() {
        var name = new TestDbTxCaller(new TxNameGenerator.Long(Set.of()))
                .getTxName();
        assertThat(name).isEqualTo("TestDbTxCaller.getTxName");
    }

    @Test
    public void testNotDbChildPackage_FromStackWalker_Auto_Long() {
        var name = new TestTxCaller(new TxNameGenerator.Long(Set.of()))
                .getTxName();
        assertThat(name).isEqualTo("TestTxCaller.getTxName");
    }

    @Test
    public void testNotDbChildPackage_FromStackWalker_Auto_Long_SkipCallerPackage() {
        var name = new TestTxCaller(new TxNameGenerator.Long(Set.of(TestTxCaller.class.getPackageName())))
                .getTxName();
        assertThat(name).isEqualTo("StdTxManagerTest.testNotDbChildPackage_FromStackWalker_Auto_Long_SkipCallerPackage");
    }

    @Test
    public void testNotDbPackage_Same_Long() {
        var nameOld = new TestTxCaller(new TxNameGenerator.Long(Set.of())).getTxName();
        var nameNew = new TestTxCaller(new TxNameGenerator.Long(Set.of())).getTxName();
        assertThat(nameNew).isEqualTo(nameOld);
    }

    @Test
    public void testNotDbPackage_ForceExplicitName_MustNotFail() {
        var name = new TestTxCaller(new TxNameGenerator.Simple("omg")).getTxName();
        assertThat(name).isEqualTo("omg");
    }

    @Test
    public void testDbPackage_ForceExplicitName_MustNotFail() {
        var name = new TestDbTxCaller(new TxNameGenerator.Simple("omg")).getTxName();
        assertThat(name).isEqualTo("omg");
    }

    @Test
    public void testDryDun_True() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);

        var txManager = new StdTxManager(repository).withDryRun(true);
        var testObj = new Object();

        assertThat(txManager.tx(() -> testObj))
                .isEqualTo(testObj);

        verify(repositoryTransaction, times(1)).rollback();
        verify(repositoryTransaction, times(0)).commit();
    }

    @Test
    public void testDryDun_False() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);

        var txManager = new StdTxManager(repository).withDryRun(false);
        var testObj = new Object();

        assertThat(txManager.tx(() -> testObj))
                .isEqualTo(testObj);

        verify(repositoryTransaction, times(0)).rollback();
        verify(repositoryTransaction, times(1)).commit();
    }

    @Test
    public void testLogStatementOnSuccess() {
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = loggerContext.getConfiguration();
        LoggerConfig loggerConfig = configuration.getLoggerConfig(TxImpl.class.getCanonicalName());

        TestAppender testAppender = new TestAppender("StdTxManagerTest.TestAppender");
        loggerConfig.addAppender(testAppender, Level.ALL, null);
        testAppender.start();
        try {
            when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
            when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
            when(transactionLocal.log()).thenReturn(transactionLog);
            var testObj = new Object();
            new StdTxManager(repository).tx(() -> testObj);
            assertThat(testAppender.getMessages()).hasSize(1);
            testAppender.clear();
            new StdTxManager(repository).withLogStatementOnSuccess(false).tx(() -> testObj);
            assertThat(testAppender.getMessages()).isEmpty();
        } finally {
            testAppender.stop();
            loggerConfig.removeAppender(testAppender.getName());
        }
    }

    @Test
    public void testDryRun_True_RetryRollback() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);
        // throw OptimisticLockException 2 times and then do nothing
        Mockito.doThrow(new OptimisticLockException("throw 1 exception"))
                .doThrow(new OptimisticLockException("throw 2 exception"))
                .doNothing()
                .when(repositoryTransaction).rollback();

        var txManager = new StdTxManager(repository).withDryRun(true);
        var testObj = new Object();

        assertThat(txManager.tx(() -> testObj))
                .isEqualTo(testObj);

        verify(repositoryTransaction, times(3)).rollback();
        verify(repositoryTransaction, times(0)).commit();
    }

    @Test
    public void testDryRun_True_RetryOptimisticLockException() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);

        var txManager = new StdTxManager(repository).withDryRun(true);
        var testObj = new Object();
        var i = new AtomicInteger();

        assertThat(txManager.tx(() -> {
            if (i.incrementAndGet() < 3) {
                throw new OptimisticLockException("lock exception");
            }
            return testObj;
        })).isEqualTo(testObj);

        verify(repositoryTransaction, times(3)).rollback();
        verify(repositoryTransaction, times(0)).commit();
    }

    @Test
    public void testDryRun_True_ThrowRuntimeException() {
        when(repository.startTransaction(any(TxOptions.class))).thenReturn(repositoryTransaction);
        when(repositoryTransaction.getTransactionLocal()).thenReturn(transactionLocal);
        when(transactionLocal.log()).thenReturn(transactionLog);

        var txManager = new StdTxManager(repository).withDryRun(true);

        assertThatThrownBy(() -> txManager.tx(() -> {
            throw new RuntimeException("runtime exception");
        })).isInstanceOf(RuntimeException.class);

        verify(repositoryTransaction, times(1)).rollback();
        verify(repositoryTransaction, times(0)).commit();
    }

    private static final class TestAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        private TestAppender(String name) {
            super(name, null, null, true, null);
        }

        public void clear() {
            messages.clear();
        }

        public List<String> getMessages() {
            return List.copyOf(messages);
        }

        @Override
        public void append(LogEvent logEvent) {
            String formattedMessage = logEvent.getMessage().getFormattedMessage();
            messages.add(formattedMessage);
        }
    }
}
