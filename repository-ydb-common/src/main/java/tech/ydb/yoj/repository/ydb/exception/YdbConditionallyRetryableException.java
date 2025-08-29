package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.ConditionallyRetryableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

import javax.annotation.Nullable;

/**
 * A <em>conditionally-retryable</em> exception from the YDB database, the YDB Java SDK, or the GRPC client used
 * by the YDB Java SDK.
 *
 * @see ConditionallyRetryableException Conditionally-retryable exceptions
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public class YdbConditionallyRetryableException extends ConditionallyRetryableException {
    private static final RetryPolicy UNDETERMINED_BACKOFF = RetryPolicy.expBackoff(5L, 500L, 0.1, 2.0);

    private final Enum<?> statusCode;

    public YdbConditionallyRetryableException(Enum<?> statusCode, Object request, Object response) {
        this("Indeterminate request state: it's not known if the request succeeded or failed",
                statusCode, request, response, UNDETERMINED_BACKOFF);
    }

    public YdbConditionallyRetryableException(String message, Enum<?> statusCode, Object request, Object response, RetryPolicy retryPolicy) {
        super(YdbRepositoryException.errorMessage(message, statusCode, request, response), retryPolicy);
        this.statusCode = statusCode;
    }

    @Nullable
    public Enum<?> getStatusCode() {
        return statusCode;
    }
}
