package tech.ydb.yoj.repository.ydb.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.GenericSchemaException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.internal.LoggableDbAction;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;
import tech.ydb.yoj.repository.ydb.exception.YdbClientInternalException;
import tech.ydb.yoj.repository.ydb.exception.YdbComponentUnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbOverloadedException;
import tech.ydb.yoj.repository.ydb.exception.YdbPreconditionFailedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.exception.YdbResultSetTooBigException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthenticatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthorizedException;
import tech.ydb.yoj.util.lang.Strings;

import java.util.function.Predicate;

import static lombok.AccessLevel.PRIVATE;

/**
 * "Classic" {@link YdbValidator YDB error mapper}, essentially unchanged during the whole YOJ 2.x series.
 * It has known problems, see <a href="https://github.com/ydb-platform/yoj-project/issues/165">GitHub Issue #165</a>.
 */
@Slf4j
@InternalApi
@RequiredArgsConstructor(access = PRIVATE)
public final class YdbValidatorClassic implements YdbValidator {
    public static final YdbValidator INSTANCE = new YdbValidatorClassic();

    @Override
    public void validate(@NonNull LoggableDbAction action, @NonNull Status status, @NonNull Object response)
            throws RepositoryException {
        StatusCode statusCode = status.getCode();
        Issue[] issues = status.getIssues();

        switch (statusCode) {
            // Success. Do nothing ;-)
            case SUCCESS -> {
            }

            // Current session can no longer be used. Retry immediately by creating a new session
            case BAD_SESSION,      // This session is no longer available. Create a new session
                 SESSION_EXPIRED,  // The session has already expired. Create a new session
                 NOT_FOUND -> {    // Prepared statement or tx was not found in current session. Create a new session
                throw new BadSessionException(response.toString());
            }

            // Transaction locks invalidated. Retry immediately
            case ABORTED -> throw new OptimisticLockException(response.toString());

            // The query cannot be executed in the current state. Non-retryable in general
            // - Primary key/UNIQUE index violations: Retry immediately (EntityAlreadyExistsException)
            // - Attempt to return a result set larger than ~50M: Non-retryable (YdbResultSetTooBigException)
            // - Other "failed preconditions" unknown to YOJ: Non-retryable (YdbPreconditionFailedException)
            case PRECONDITION_FAILED -> {
                if (is(issues, IssueCode.CONSTRAINT_VIOLATION::matches)) {
                    throw new EntityAlreadyExistsException("Entity already exists" + errorMessageFrom(issues));
                } else if (is(issues, IssueCode.QUERY_RESULT_SIZE_EXCEEDED::matches)) {
                    throw new YdbResultSetTooBigException(action, response);
                } else {
                    throw new YdbPreconditionFailedException(action, response);
                }
            }

            // DB overloaded and similar conditions. Slow retry with exponential backoff
            case OVERLOADED,
                 // DB took too long to respond
                 TIMEOUT,
                 // The request was cancelled because the request timeout (CancelAfter) has expired.
                 // The request has been cancelled on the server
                 CANCELLED,
                 // Not enough resources to process the request
                 CLIENT_RESOURCE_EXHAUSTED,
                 // Deadline expired before the request was sent to the server
                 CLIENT_DEADLINE_EXPIRED,
                 // The request was cancelled on the client, at the transport level (because the GRPC deadline expired)
                 CLIENT_DEADLINE_EXCEEDED -> {
                YdbOperations.checkGrpcDeadlineAndCancellation(response.toString(), null);

                // The result of the request is unknown; it might have been cancelled... or it executed successfully!
                log.warn("""
                        Database is overloaded, but we still got a reply from the DB
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbOverloadedException(action, response);
            }

            // Unknown error on the client side (most often at the transport level). Fast retry with fixed interval
            case CLIENT_CANCELLED,
                 CLIENT_GRPC_ERROR,
                 CLIENT_INTERNAL_ERROR -> {
                YdbOperations.checkGrpcDeadlineAndCancellation(response.toString(), null);

                log.warn("""
                        YDB SDK internal error or cancellation
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbClientInternalException(action, response);
            }

            // DB, one of its components, or the transport is temporarily unavailable. Fast retry with fixed interval
            case UNAVAILABLE,               // DB responded that it or some of its subsystems are unavailable
                 TRANSPORT_UNAVAILABLE,     // Network connectivity issues
                 CLIENT_DISCOVERY_FAILED,   // Error occurred while retrieving the list of endpoints
                 CLIENT_LIMITS_REACHED,     // Client-side session limit reached
                 SESSION_BUSY,              // Another query is being executed in this session, retry with a new session
                 UNDETERMINED -> {
                // FIXME(nvamelichev): SESSION_BUSY is here only because it needs a retry with some delay, not immediate
                // We should handle SESSION_BUSY separately, e.g. by adding a RetryPolicy arg to BadSessionException
                // (see https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes?version=v25.2)

                YdbOperations.checkGrpcDeadlineAndCancellation(response.toString(), null);

                log.warn("""
                        Some database components are not available, but we still got a reply from the DB
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbComponentUnavailableException(action, response);
            }

            // GRPC client reports that the request was not authenticated properly. Retry immediately
            // This is an internal error, but there may have been an issue with issuing the token, so we retry.
            // If that doesn’t work, we will propagate the error quickly enough.
            case CLIENT_UNAUTHENTICATED -> {
                log.warn("""
                        Unauthenticated request to the database
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbUnauthenticatedException(action, response);
            }

            // DB reports that the request is unauthorized. Retry immediately
            // Retries might help in case e.g. access permissions are updated in an eventually-consistent way
            case UNAUTHORIZED -> {
                log.warn("""
                        Unauthorized request to the database
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbUnauthorizedException(action, response);
            }

            // Database schema problem, e.g. trying to access a non-existent table, column etc. No retries
            case SCHEME_ERROR -> throw new GenericSchemaException(Strings.join("\n", "Schema error", action, response));

            // Serious internal error. No retries
            case CLIENT_CALL_UNIMPLEMENTED,
                 BAD_REQUEST,
                 UNSUPPORTED,
                 INTERNAL_ERROR,
                 GENERIC_ERROR,
                 UNUSED_STATUS,
                 ALREADY_EXISTS -> { // Not used by {Table,Query}Service. We consider it fatal
                log.error("""
                        Bad response status
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbRepositoryException(action, response);
            }

            // Unknown YDB status code. No retries
            default -> {
                log.error("""
                        Unknown YDB status code, treating as non-retryable
                        Request: {}
                        Response: {}""", action, response);
                throw new YdbRepositoryException(action, response);
            }
        }
    }

    @Override
    public boolean isTransactionClosedByServer(@NonNull Status status) {
        return switch (status.getCode()) {
            case UNUSED_STATUS,
                 ALREADY_EXISTS,
                 BAD_REQUEST,
                 UNAUTHORIZED,
                 INTERNAL_ERROR,
                 ABORTED,
                 UNAVAILABLE,
                 OVERLOADED,
                 SCHEME_ERROR,
                 GENERIC_ERROR,
                 TIMEOUT,
                 BAD_SESSION,
                 PRECONDITION_FAILED,
                 NOT_FOUND,
                 SESSION_EXPIRED,
                 CANCELLED,
                 UNDETERMINED,
                 UNSUPPORTED,
                 SESSION_BUSY,
                 EXTERNAL_ERROR -> true;
            case SUCCESS,
                 TRANSPORT_UNAVAILABLE,
                 CLIENT_CANCELLED,
                 CLIENT_CALL_UNIMPLEMENTED,
                 CLIENT_DEADLINE_EXCEEDED,
                 CLIENT_INTERNAL_ERROR,
                 CLIENT_UNAUTHENTICATED,
                 CLIENT_DEADLINE_EXPIRED,
                 CLIENT_DISCOVERY_FAILED,
                 CLIENT_LIMITS_REACHED,
                 CLIENT_RESOURCE_EXHAUSTED,
                 CLIENT_GRPC_ERROR -> false;
        };
    }

    private static boolean is(Issue[] issues, Predicate<Issue> predicate) {
        for (Issue issue : issues) {
            if (predicate.test(issue) || (issue.getIssues().length > 0 && is(issue.getIssues(), predicate))) {
                return true;
            }
        }
        return false;
    }

    private static String errorMessageFrom(Issue[] issues) {
        StringBuilder sb = new StringBuilder();
        for (Issue issue : issues) {
            sb.append(": ");
            sb.append(issue.getMessage());
        }
        return sb.toString();
    }

    @RequiredArgsConstructor(access = PRIVATE)
    private enum IssueCode {
        CONSTRAINT_VIOLATION(2012),
        QUERY_RESULT_SIZE_EXCEEDED(2013)
        ;

        private final int issueCode;

        public boolean matches(Issue msg) {
            return msg.getCode() == issueCode;
        }
    }
}
