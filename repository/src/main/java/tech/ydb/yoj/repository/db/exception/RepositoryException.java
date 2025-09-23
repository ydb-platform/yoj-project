package tech.ydb.yoj.repository.db.exception;

/**
 * Base class for all database access exceptions. Instances of this class are treated as non-retryable by default,
 * unless they are subclasses of {@link RetryableException}.
 */
@SuppressWarnings("checkstyle:LeftCurly")
public sealed abstract class RepositoryException
        extends RuntimeException
        permits
            // final
            ConversionException,
            CreateTableException,
            DropTableException,
            DeadlineExceededException,
            InternalRepositoryException,
            QueryCancelledException,
            QueryInterruptedException,
            UnavailableException,
            // sealed
            IllegalTransactionException,
            // non-sealed
            RetryableException,
            ImplementationSpecificRepositoryException
{
    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
