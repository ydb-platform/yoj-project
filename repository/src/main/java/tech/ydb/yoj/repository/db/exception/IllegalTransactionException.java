package tech.ydb.yoj.repository.db.exception;

public class IllegalTransactionException extends RepositoryException {
    public IllegalTransactionException(String message) {
        super(message);
    }
}
