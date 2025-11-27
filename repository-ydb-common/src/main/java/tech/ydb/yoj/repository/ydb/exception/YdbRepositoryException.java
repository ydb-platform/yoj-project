package tech.ydb.yoj.repository.ydb.exception;

import tech.ydb.yoj.repository.db.exception.ImplementationSpecificRepositoryException;
import tech.ydb.yoj.util.lang.Strings;

import javax.annotation.Nullable;

/**
 * A generic non-retryable exception from the YDB database, the YDB Java SDK, or the GRPC client used
 * by the YDB Java SDK.
 */
public abstract sealed class YdbRepositoryException
        extends ImplementationSpecificRepositoryException
        permits ResultTruncatedException, YdbPreconditionFailedException, YdbInternalException, UnexpectedException {
    private final Enum<?> statusCode;

    public YdbRepositoryException(Enum<?> statusCode, Object request, Object response) {
        this(null, statusCode, request, response);
    }

    public YdbRepositoryException(String message, Object request, Object response) {
        this(message, null, request, response);
    }

    public YdbRepositoryException(String message, Enum<?> statusCode, Object request, Object response) {
        this(message, statusCode, request, response, null);
    }

    public YdbRepositoryException(String message, Enum<?> statusCode, Object request, Object response, Throwable cause) {
        super(errorMessage(message, statusCode, request, response), cause);
        this.statusCode = statusCode;
    }

    /*package*/ static String errorMessage(
            @Nullable String message,
            @Nullable Enum<?> statusCode,
            @Nullable Object request,
            @Nullable Object response
    ) {
        return Strings.join("\n", Strings.join(" | ", statusCode, message), request, response);
    }

    @Nullable
    public Enum<?> getStatusCode() {
        return statusCode;
    }
}
