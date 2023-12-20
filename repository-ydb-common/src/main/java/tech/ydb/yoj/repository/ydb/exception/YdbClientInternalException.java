package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * Internal YDB SDK exception, caused by a transport failure, internal authorization/authentication error etc.
 */
public final class YdbClientInternalException extends RetryableException {
    private static final RetryPolicy RETRY_POLICY = RetryPolicy.fixed(100L, 0.2);

    public YdbClientInternalException(Object request, Object response) {
        super(Strings.join("\n", request, response), RETRY_POLICY);
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("YDB SDK internal exception, retries failed", this);
    }
}
