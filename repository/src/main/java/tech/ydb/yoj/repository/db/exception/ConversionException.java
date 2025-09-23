package tech.ydb.yoj.repository.db.exception;

/**
 * Thrown when the repository cannot convert a raw database row to entity, or vice versa.
 */
public final class ConversionException extends RepositoryException {
    public ConversionException(String message) {
        super(message);
    }

    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
