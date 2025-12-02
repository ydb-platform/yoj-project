package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * YDB authorization failure, possibly a transient one. E.g., the principal tried to write to the database but has no
 * write-allowing role assigned.
 */
public class YdbUnauthorizedException extends YdbUnconditionallyRetryableException {
    public YdbUnauthorizedException(Enum<?> statusCode, Object request, Object response) {
        super("Access to database denied", statusCode, request, response, RetryPolicy.retryImmediately());
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Access to database denied, retries failed", this);
    }
}
