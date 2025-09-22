package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.lang.Strings;
import tech.ydb.yoj.util.retry.RetryPolicy;

/**
 * YDB authentication failure, possibly a transient one. E.g., used a recently expired token.
 */
public class YdbUnauthenticatedException extends RetryableException {
    public YdbUnauthenticatedException(Object request, Object response) {
        super(Strings.join("\n", request, response), RetryPolicy.retryImmediately());
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("DB authentication failed after retries", this);
    }
}
