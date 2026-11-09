package tech.ydb.yoj.repository.ydb.client;

import lombok.NonNull;
import tech.ydb.core.Status;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction.FinalDbAction;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction.InitialDbAction;

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
     * @param action   Loggable information about the action being performed. You may match on the action type via
     *                 {@code instanceof} because {@code LoggableDbAction} is a {@code sealed interface}; or even match
     *                 the whole action (via {@code ==}) against known constants, in a limited number of cases:
     *                 <ul>
     *                 <li>{@link InitialDbAction#GET_SESSION GET_SESSION}</li>
     *                 <li>{@link FinalDbAction#COMMIT COMMIT}</li>
     *                 <li>{@link FinalDbAction#ROLLBACK ROLLBACK}</li>
     *                 </ul>
     * @param status   YDB status of the response
     * @param response YDB response object with a sane {@code toString()}, <strong>NOT</strong> machine-readable
     * @throws RepositoryException error corresponding to the {@code status}
     */
    void validate(
            @NonNull LoggableDbAction action,
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
