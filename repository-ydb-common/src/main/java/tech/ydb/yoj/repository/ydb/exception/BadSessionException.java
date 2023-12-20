package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.RetryableException;

/**
 * Tried to use a no longer active or valid YDB session, e.g. on a node that is now down.
 */
public class BadSessionException extends RetryableException {
    public BadSessionException(String message) {
        super(message);
    }
}
