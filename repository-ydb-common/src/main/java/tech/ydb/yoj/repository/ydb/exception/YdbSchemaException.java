package tech.ydb.yoj.repository.ydb.exception;

/**
 * Base class for database schema problems, e.g. table not found.
 */
public class YdbSchemaException extends YdbRepositoryException {
    public YdbSchemaException(String message, Object request, Object response) {
        super(message, request, response);
    }

    public YdbSchemaException(String message) {
        super(message);
    }
}
