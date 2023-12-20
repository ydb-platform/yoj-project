package tech.ydb.yoj.repository.db.exception;

import tech.ydb.yoj.repository.db.IsolationLevel;

import static java.lang.String.format;

public class IllegalTransactionIsolationLevelException extends IllegalTransactionException {
    public IllegalTransactionIsolationLevelException(String message, IsolationLevel isolationLevel) {
        super(format("%s are not allowed for isolation level %s", message, isolationLevel));
    }
}
