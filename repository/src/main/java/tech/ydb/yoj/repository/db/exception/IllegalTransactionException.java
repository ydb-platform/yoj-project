package tech.ydb.yoj.repository.db.exception;

@SuppressWarnings("checkstyle:LeftCurly")
public sealed abstract class IllegalTransactionException
        extends RepositoryException
        permits IllegalTransactionIsolationLevelException, IllegalTransactionScanException
{
    public IllegalTransactionException(String message) {
        super(message);
    }
}
