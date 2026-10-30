package tech.ydb.yoj.repository.ydb.client;

import io.grpc.Context;
import lombok.NonNull;
import tech.ydb.core.Status;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;

import javax.annotation.Nullable;

/**
 * Mapper from YDB {@code Status}es into {@code RepositoryException}s.
 * <br>Should not depend on YDB transport, YDB session, or YOJ transaction-local context. It should, however, check for
 * {@link io.grpc.Context#current() GRPC cancellation} to have fail-fast handling of <strong>retryable</strong> errors,
 * via the {@link #checkGrpcDeadlineAndCancellation(String, Throwable) checkGrpcDeadlineAndCancellation()} method.
 *
 * <p><strong>WARNING: This is an Experimental low-level API.</strong> Please use as advised by YOJ developers.
 * This API might change at any time or disappear entirely <strong>without any prior notice!</strong>
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public interface YdbValidator {
    /**
     * {@code YdbValidator} implementation used by default, if you don't customize it in {@code YdbRepository}.<br>
     * It's <strong>strongly recommended</strong> to delegate to this implementation in your own {@code YdbValidator}
     * implementations, instead of reimplementing the whole error mapping logic by yourself.
     */
    YdbValidator DEFAULT = YdbValidatorClassic.INSTANCE;

    /**
     * Validates that {@code status} is successful, and throws an appropriate subtype of {@link RepositoryException}
     * if {@code status} is an error.
     *
     * @param request  YDB request being made. Not machine-readable in general, but may be checked against known
     *                 constants in a limited number of cases:
     *                 <ul>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.client.YdbSessionManager#REQUEST_GET_SESSION}</li>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction#REQUEST_COMMIT}</li>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction#REQUEST_ROLLBACK}</li>
     *                 </ul>
     * @param status   YDB status of the response
     * @param response YDB response string, not machine-readable
     * @throws RepositoryException error corresponding to the {@code status}
     */
    void validate(String request, Status status, String response) throws RepositoryException;

    /**
     * Checks if an error {@code status} resulted in a termination of YDB transaction.
     *
     * @param status YDB status
     * @return {@code true} if YDB transaction is no longer valid after receiving this {@code status}; {@code false} otherwise
     */
    boolean isTransactionClosedByServer(Status status);

    /**
     * Checks current GRPC context for timeout and cancellation, and throws an appropriate {@code RepositoryException}
     * if the context indeed timed out or was cancelled.
     *
     * @param errorMessage error message from the database; might be {@code null}
     * @param cause        an exception that caused the timeout and cancellation check; might be {@code null}
     * @throws DeadlineExceededException Deadline for current GRPC request context was exceeded
     * @throws QueryCancelledException   Current GRPC request context was cancelled
     */
    static void checkGrpcDeadlineAndCancellation(@Nullable String errorMessage, @Nullable Throwable cause) {
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
