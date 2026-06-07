package tech.ydb.yoj.repository.ydb;

import io.grpc.Context;
import lombok.NonNull;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.InternalRepositoryException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static tech.ydb.yoj.util.lang.Interrupts.isThreadInterrupted;

@InternalApi
public final class YdbOperations {
    private YdbOperations() {
    }

    public static <T> T safeJoin(CompletableFuture<T> future) {
        return safeJoin(future, Duration.ofMinutes(5)); //todo: config
    }

    public static <T> T safeJoin(CompletableFuture<T> future, Duration timeout) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw convertToRepositoryException(e);
        }
    }

    private static RepositoryException convertToUnavailable(Throwable ex) {
        if (isThreadInterrupted(ex)) {
            Thread.currentThread().interrupt();
            return new QueryInterruptedException("DB query interrupted", ex);
        }
        checkGrpcDeadlineAndCancellation(ex.getMessage(), ex);

        return new UnavailableException("DB is unavailable", ex);
    }

    public static RepositoryException convertToRepositoryException(Throwable ex) {
        if (ex instanceof CancellationException) {
            return convertToUnavailable(ex);
        } else if (ex instanceof CompletionException) {
            return convertToUnavailable(ex);
        } else if (ex instanceof InterruptedException) {
            return convertToUnavailable(ex);
        } else if (ex instanceof TimeoutException) {
            return convertToUnavailable(ex);
        } else if (ex instanceof ExecutionException) {
            return new YdbRepositoryException("ExecutionException was caught", ex.getCause());
        } else if (ex instanceof RepositoryException) {
            return (RepositoryException) ex;
        } else {
            return new InternalRepositoryException(ex);
        }
    }

    /**
     * Checks current GRPC context for timeout and cancellation, and throws an appropriate {@code RepositoryException}
     * if the context indeed timed out or was cancelled.
     *
     * @param errorMessage error message from the database; might be {@code null}
     * @param cause        an exception that caused the timeout and cancellation check; might be {@code null}
     * @throws DeadlineExceededException Deadline for current GRPC request context was exceeded
     * @throws QueryCancelledException   Current GRPC request context was cancelled
     */
    public static void checkGrpcDeadlineAndCancellation(@Nullable String errorMessage, @Nullable Throwable cause) {
        Context ctx = Context.current();
        if (ctx.getDeadline() != null && ctx.getDeadline().isExpired()) {
            // GRPC deadline for the current GRPC context has expired. We need to throw a separate exception to avoid retries
            throw new DeadlineExceededException("DB query deadline exceeded" + responseToString(errorMessage), cause);
        } else if (ctx.isCancelled()) {
            // Client has cancelled the GRPC request. Throw a separate exception to avoid retries
            throw new QueryCancelledException("DB query cancelled" + responseToString(errorMessage));
        }
    }

    @NonNull
    private static String responseToString(@Nullable String response) {
        return response == null ? "" : ". Response from DB: " + response;
    }
}
