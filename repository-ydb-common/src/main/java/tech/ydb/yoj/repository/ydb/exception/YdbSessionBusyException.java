package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.util.retry.RetryPolicy;

public class YdbSessionBusyException extends BadSessionException {
    private static final RetryPolicy RETRY_POLICY = RetryPolicy.expBackoff(
            /*initial*/ 5L,
            /*max*/ 500L,
            /*jitter*/ 0.1,
            /*multiplier*/ 2.0
    );

    public YdbSessionBusyException(Enum<?> statusCode, Object request, Object response) {
        super(statusCode, RETRY_POLICY, request, response);
    }
}
