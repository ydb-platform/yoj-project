package tech.ydb.yoj.repository.db.exception;

/**
 * Thrown if the thread awaiting the query's results has been interrupted.
 */
public final class QueryInterruptedException extends RepositoryException {
    public QueryInterruptedException(String message) {
        super(message);
    }

    public QueryInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
