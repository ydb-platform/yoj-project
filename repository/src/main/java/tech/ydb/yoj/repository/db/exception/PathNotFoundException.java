package tech.ydb.yoj.repository.db.exception;

public final class PathNotFoundException extends SchemaException {
    public PathNotFoundException(String message) {
        super(message);
    }
}
