package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.core.Status;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.RepositoryException;

/**
 * Mapper from YDB {@code Status}es into {@code RepositoryException}s.
 * <br>Assumed to be stateless (not dependent on YDB transport, YDB session, YOJ transaction-local context,
 * GRPC context or whatever.)
 *
 * <p><strong>WARNING: This is an Experimental low-level API.</strong> Please use as advised by YOJ developers.
 * This API might change at any time or disappear entirely <strong>without any prior notice!</strong>
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public interface YdbValidator {
    YdbValidator DEFAULT = YdbValidatorImpl.INSTANCE;

    /**
     * Validates that {@code status} is successful, and throws an appropriate subtype of {@link RepositoryException}
     * if {@code status} is an error.
     *
     * @param request  YDB request being made. Not machine-readable in general, but may be checked against known
     *                 constants in a limited number of cases, <em>e.g.</em>:
     *                 <ul>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.client.YdbSessionManager#REQUEST_GET_SESSION acquire session from pool/create new session},</li>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction#REQUEST_COMMIT commit transaction},</li>
     *                 <li>{@link tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction#REQUEST_ROLLBACK rollback transaction}</li>
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
}
