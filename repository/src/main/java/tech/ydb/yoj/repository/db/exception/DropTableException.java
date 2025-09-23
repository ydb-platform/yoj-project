package tech.ydb.yoj.repository.db.exception;

public final class DropTableException extends SchemaException {
    public DropTableException(String msg) {
        super(msg);
    }
}
