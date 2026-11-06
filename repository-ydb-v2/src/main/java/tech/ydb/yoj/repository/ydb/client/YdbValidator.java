package tech.ydb.yoj.repository.ydb.client;

import lombok.NonNull;
import tech.ydb.core.Status;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.internal.DbRequest;

/**
 * Mapper from YDB {@code Status}es into {@code RepositoryException}s.
 * <br>Should not depend on YDB transport, YDB session, or YOJ transaction-local context. It should, however, check for
 * {@link io.grpc.Context#current() GRPC cancellation} to have fail-fast handling of <strong>retryable</strong> errors,
 * via the {@link YdbOperations#checkGrpcDeadlineAndCancellation(String, Throwable)} method.
 *
 * <p><strong>WARNING: This is an Experimental API.</strong> Please use as advised by YOJ developers.
 * This API might change at any time or disappear entirely <strong>without any prior notice!</strong>
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public interface YdbValidator {
    /**
     * Validates that {@code status} is successful, and throws an appropriate subtype of {@link RepositoryException}
     * if {@code status} is an error.
     *
     * @param request  Information about YDB request being made. You may inspect its {@link DbRequest#getType()} type},
     *                 and even the whole request may be matched (via {@link DbRequest#equals(Object)}) against known
     *                 constants (in a limited number of cases):
     *                 <ul>
     *                 <li>{@link tech.ydb.yoj.repository.db.internal.DbRequest#COMMIT}</li>
     *                 <li>{@link tech.ydb.yoj.repository.db.internal.DbRequest#ROLLBACK}</li>
     *                 <li>{@link tech.ydb.yoj.repository.db.internal.DbRequest#GET_SESSION}</li>
     *                 </ul>
     * @param status   YDB status of the response
     * @param response YDB response string, not machine-readable
     * @throws RepositoryException error corresponding to the {@code status}
     */
    void validate(
            @NonNull DbRequest request,
            @NonNull Status status,
            @NonNull Object response
    ) throws RepositoryException;

    /**
     * Checks if an error {@code status} resulted in a termination of YDB transaction.
     *
     * @param status YDB status
     * @return {@code true} if YDB transaction is no longer valid after receiving this {@code status}; {@code false} otherwise
     */
    boolean isTransactionClosedByServer(@NonNull Status status);
}
