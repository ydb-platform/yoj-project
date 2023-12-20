package tech.ydb.yoj.repository.db.exception;

import lombok.Getter;

public class UnavailableException extends RepositoryException {
    @Getter
    public final boolean alreadyRetried;

    public UnavailableException(String message) {
        super(message);
        this.alreadyRetried = false;
    }

    public UnavailableException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public UnavailableException(String message, Throwable cause, boolean alreadyRetried) {
        super(message, cause);
        this.alreadyRetried = alreadyRetried;
    }

    public static UnavailableException afterRetries(String message, Throwable cause) {
        return new UnavailableException(message, cause, true);
    }
}
