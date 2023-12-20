package tech.ydb.yoj.repository.db.list;

import lombok.Getter;

public abstract class BadListingException extends IllegalArgumentException {
    protected BadListingException(String message) {
        super(message);
    }

    protected BadListingException(String message, Throwable cause) {
        super(message, cause);
    }

    @Getter
    public static final class BadPageSize extends BadListingException {
        private final long pageSize;
        private final long maxPageSize;

        public BadPageSize(long pageSize, long maxPageSize) {
            super("Invalid page size (%d). Must be between and 1 and %d, inclusive".formatted(pageSize, maxPageSize));
            this.pageSize = pageSize;
            this.maxPageSize = maxPageSize;
        }
    }

    @Getter
    public static final class BadOffset extends BadListingException {
        private final long maxSkipSize;

        public BadOffset(long maxSkipSize) {
            super("Invalid page token. Paging more than %d results in total is not supported".formatted(maxSkipSize));
            this.maxSkipSize = maxSkipSize;
        }
    }

    public static final class InvalidPageToken extends BadListingException {
        public InvalidPageToken() {
            this(null);
        }

        public InvalidPageToken(Throwable cause) {
            super("Invalid page token", cause);
        }
    }
}
