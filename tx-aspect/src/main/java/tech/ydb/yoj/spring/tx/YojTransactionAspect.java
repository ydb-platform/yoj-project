package tech.ydb.yoj.spring.tx;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.exception.RetryableException;

/**
 * The aim of the class is to eliminate direct <code>TxManager</code> usage
 */
@Aspect
public class YojTransactionAspect {
    private final TxManager tx;

    public YojTransactionAspect(TxManager tx) {
        this.tx = tx;
    }

    /**
     * Annotation on the methos has priority over class
     */
    @Around("@within(transactional) && !@annotation(YojTransactional)")
    public Object doInClassTransaction(ProceedingJoinPoint pjp, YojTransactional transactional) throws Throwable {
        return doInTransaction(pjp, transactional);
    }

    @Around("@annotation(transactional)")
    public Object doInMethodTransaction(ProceedingJoinPoint pjp, YojTransactional transactional) throws Throwable {
        return doInTransaction(pjp, transactional);
    }

    private Object doInTransaction(ProceedingJoinPoint pjp, YojTransactional transactional) throws Throwable {
        try {
            String name = transactional.name().isBlank() ? getSimpleMethod(pjp.getSignature()) : transactional.name();
            TxManager localTx = tx.withName(name);
            if (Tx.Current.exists()) {
                if (transactional.propagation() == YojTransactional.Propagation.NEVER) {
                    throw new IllegalStateException("Transaction already exist!");
                } else if (transactional.propagation() == YojTransactional.Propagation.REQUIRED) {
                    return pjp.proceed();
                } else if (transactional.propagation() == YojTransactional.Propagation.REQUIRES_NEW) {
                    localTx = localTx.separate();
                }
            }
            if (transactional.maxRetries() != YojTransactional.UNDEFINED) {
                localTx = tx.withMaxRetries(transactional.maxRetries());
            }
            return transactional.readOnly() ? localTx.readOnly().run(() -> safeCall(pjp)) : localTx.tx(() -> safeCall(pjp));
        } catch (CallRetryableException | CallException e) {
            throw e.getCause();
        }
    }

    private static String getSimpleMethod(Signature signature) {
        return signature.getDeclaringType().getSimpleName() + "." + signature.getName();
    }

    Object safeCall(ProceedingJoinPoint pjp) {
        try {
            return pjp.proceed();
        } catch (RetryableException e) {
            throw new CallRetryableException(e);
        } catch (Throwable e) {
            throw new CallException(e);
        }
    }

    /**
     * It's a hint for tx manager to retry was requested
     */
    static class CallRetryableException extends RetryableException {
        CallRetryableException(RetryableException e) {
            super(e.getMessage(), e.getCause());
        }
    }

    static class CallException extends RuntimeException {
        CallException(Throwable e) {
            super(e);
        }
    }
}
