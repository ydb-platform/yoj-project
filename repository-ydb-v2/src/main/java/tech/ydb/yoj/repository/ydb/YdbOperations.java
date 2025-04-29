package tech.ydb.yoj.repository.ydb;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.InternalRepositoryException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static tech.ydb.yoj.repository.ydb.client.YdbValidator.checkGrpcContextStatus;
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
        checkGrpcContextStatus(ex.getMessage(), ex);

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
}
