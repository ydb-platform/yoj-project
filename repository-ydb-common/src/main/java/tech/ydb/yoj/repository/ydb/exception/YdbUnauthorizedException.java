package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.util.lang.Strings;

/**
 * YDB authorization failure, possibly a transient one. E.g., the principal tried to write to the database but has no
 * write-allowing role assigned.
 */
public class YdbUnauthorizedException extends RetryableException {
    public YdbUnauthorizedException(Object request, Object response) {
        super(Strings.join("\n", request, response));
    }

    @Override
    public RepositoryException rethrow() {
        return UnavailableException.afterRetries("Access to database denied, retries failed", this);
    }
}
