package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Tried to use a no longer active or valid YDB session, e.g. on a node that is now down.
 */
public class BadSessionException extends YdbUnconditionallyRetryableException {
    public BadSessionException(Enum<?> statusCode, Object request, Object response) {
        super("Bad session", statusCode, request, response, RetryPolicy.retryImmediately());
    }
}
