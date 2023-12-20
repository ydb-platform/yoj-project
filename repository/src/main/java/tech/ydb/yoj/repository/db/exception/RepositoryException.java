package tech.ydb.yoj.repository.db.exception;

/**
 * Base class for all database access exceptions. Instances of this class are treated as non-retryable by default,
 * unless they are subclasses of {@link RetryableException}.
 */
public abstract class RepositoryException extends RuntimeException {
    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
