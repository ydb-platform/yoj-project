package tech.ydb.yoj.aspect.tx;

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
                    throw new IllegalStateException(
                            "Transaction already exists (tried to start transaction '" + name + "')"
                    );
                } else if (transactional.propagation() == YojTransactional.Propagation.REQUIRED) {
                    return pjp.proceed();
                } else if (transactional.propagation() == YojTransactional.Propagation.REQUIRES_NEW) {
                    localTx = localTx.separate();
                }
            }

            Class<? extends Throwable>[] noRollbackExceptions = transactional.noRollbackFor();
            for (Class<? extends Throwable> t : noRollbackExceptions) {
                if (RetryableException.class.isAssignableFrom(t)) {
                    throw new IllegalStateException(
                            "RetryableExceptions always cause rollback, you cannot add them to noRollbackExceptions"
                    );
                }
            }

            if (transactional.maxRetries() != YojTransactional.UNDEFINED) {
                localTx = localTx.withMaxRetries(transactional.maxRetries());
            }

            validateIsolationLevel(name, transactional);

            if (transactional.readOnly()) {
                return localTx
                    .readOnly()
                    .noFirstLevelCache()
                    .withStatementIsolationLevel(transactional.isolation())
                    .run(() -> safeCall(pjp, transactional.noRollbackFor()))
                    .reThrowSkipped();
            } else {
                localTx = switch (transactional.writeMode()) {
                    case UNSPECIFIED -> localTx;
                    case DELAYED -> localTx.delayedWrites();
                    case IMMEDIATE -> localTx.immediateWrites();
                };

                return localTx.tx(() -> safeCall(pjp, transactional.noRollbackFor()))
                    .reThrowSkipped();
            }
        } catch (CallRetryableException | CallException e) {
            throw e.getCause();
        }
    }

    private void validateIsolationLevel(String name, YojTransactional transactional) {
        if (transactional.isolation().isReadWrite() && transactional.readOnly()) {
            throw new IllegalStateException(
                    "Unsupported isolation level for read-only transaction '" + name + "': " + transactional.isolation()
            );
        }
    }

    private static String getSimpleMethod(Signature signature) {
        return signature.getDeclaringType().getSimpleName() + "." + signature.getName();
    }

    CallResult safeCall(ProceedingJoinPoint pjp, Class<? extends Throwable>[] noRollbackExceptions) {
        try {
            return CallResult.ofSuccess(pjp.proceed());
        } catch (RetryableException e) {
            throw new CallRetryableException(e);
        } catch (Throwable e) {
            for (Class<? extends Throwable> t : noRollbackExceptions) {
                if (t.isAssignableFrom(e.getClass())) {
                    return CallResult.ofError(e);
                }
            }
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

    record CallResult(Object result, Throwable error) {
        public static CallResult ofSuccess(Object r) {
            return new CallResult(r, null);
        }

        public static CallResult ofError(Throwable e) {
            return new CallResult(null, e);
        }

        Object reThrowSkipped() throws Throwable {
            if (error != null) {
                throw error;
            }
            return result;
        }
    }
}
