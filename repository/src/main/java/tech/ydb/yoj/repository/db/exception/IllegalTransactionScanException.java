package tech.ydb.yoj.repository.db.exception;

import static java.lang.String.format;

public final class IllegalTransactionScanException extends IllegalTransactionException {
    public IllegalTransactionScanException(String message) {
        super(format("%s are not allowed in scan transaction", message));
    }
}
