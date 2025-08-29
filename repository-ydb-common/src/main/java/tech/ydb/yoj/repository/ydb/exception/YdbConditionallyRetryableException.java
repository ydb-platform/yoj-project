package tech.ydb.yoj.repository.ydb.exception;

import lombok.Getter;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.ConditionallyRetryableException;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Base class for <em>conditionally-retryable</em> exceptions from the YDB database, the YDB Java SDK, and the GRPC client used by the YDB Java SDK.
 *
 * @see ConditionallyRetryableException Conditionally-retryable Exceptions
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public class YdbConditionallyRetryableException extends ConditionallyRetryableException {
    private static final RetryPolicy UNDETERMINED_BACKOFF = RetryPolicy.expBackoff(5L, 500L, 0.1, 2.0);

    @Getter
    private final Enum<?> statusCode;

    public YdbConditionallyRetryableException(String message, Enum<?> statusCode, Object request, Object response) {
        this(message, statusCode, request, response, UNDETERMINED_BACKOFF);
    }

    public YdbConditionallyRetryableException(String message, Enum<?> statusCode, Object request, Object response, RetryPolicy retryPolicy) {
        super(Strings.join("\n", "[" + statusCode + "] " + message, request, response), retryPolicy);
        this.statusCode = statusCode;
    }
}
