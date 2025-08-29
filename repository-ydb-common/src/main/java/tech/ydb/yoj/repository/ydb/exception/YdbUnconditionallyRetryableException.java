package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

import javax.annotation.Nullable;

/**
 * A <em>conditionally-retryable</em> exception from the YDB database, the YDB Java SDK, or the GRPC client used
 * by the YDB Java SDK.
 *
 * @see RetryableException Unconditionally retryable exceptions
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public abstract class YdbUnconditionallyRetryableException extends RetryableException {
    private final Enum<?> statusCode;

    public YdbUnconditionallyRetryableException(String message, Enum<?> statusCode, Object request, Object response, RetryPolicy retryPolicy) {
        super(YdbRepositoryException.errorMessage(message, statusCode, request, response), retryPolicy);
        this.statusCode = statusCode;
    }

    @Nullable
    public Enum<?> getStatusCode() {
        return statusCode;
    }
}
