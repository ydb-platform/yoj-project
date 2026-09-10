package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

public class OptimisticLockException extends RetryableException {
    public OptimisticLockException(String message) {
        super(message, RetryPolicy.retryImmediately());
    }

    public OptimisticLockException(String message, Throwable cause) {
        super(message, RetryPolicy.retryImmediately(), cause);
    }
}
