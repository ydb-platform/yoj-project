package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.ImplementationSpecificRepositoryException;
import tech.ydb.yoj.util.lang.Strings;

/**
 * Base class for non-retryable YDB-specific exceptions.
 */
// TODO: make abstract
@SuppressWarnings("checkstyle:LeftCurly")
public sealed class YdbRepositoryException
        extends ImplementationSpecificRepositoryException
        permits ResultTruncatedException, UnexpectedException
{
    public YdbRepositoryException(Object request, Object response) {
        this(null, request, response);
    }

    public YdbRepositoryException(String message, Object request, Object response) {
        this(Strings.join("\n", message, request, response));
    }

    public YdbRepositoryException(String message) {
        super(message);
    }

    public YdbRepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
