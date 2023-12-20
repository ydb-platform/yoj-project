package tech.ydb.yoj.repository.db.exception;

public class QueryCancelledException extends RepositoryException {
    public QueryCancelledException(String message) {
        super(message);
    }

    public QueryCancelledException(String message, Throwable cause) {
        super(message, cause);
    }
}
