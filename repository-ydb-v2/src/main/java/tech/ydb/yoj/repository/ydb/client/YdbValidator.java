package tech.ydb.yoj.repository.ydb.client;

import io.grpc.Context;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.GenericSchemaException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;
import tech.ydb.yoj.repository.ydb.exception.UnexpectedException;
import tech.ydb.yoj.repository.ydb.exception.YdbComponentUnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbConditionallyRetryableException;
import tech.ydb.yoj.repository.ydb.exception.YdbInternalException;
import tech.ydb.yoj.repository.ydb.exception.YdbOverloadedException;
import tech.ydb.yoj.repository.ydb.exception.YdbPreconditionFailedException;
import tech.ydb.yoj.repository.ydb.exception.YdbSessionNotAcquiredException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthenticatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthorizedException;
import tech.ydb.yoj.util.lang.Strings;

import javax.annotation.Nullable;
import java.util.function.Predicate;

import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.ydb.client.YdbSessionManager.REQUEST_CREATE_SESSION;

/**
 * @see <a href="https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes">YDB Server Status Codes</a>
 * @see <a href="https://ydb.tech/docs/en/reference/ydb-sdk/grpc-status-codes">YDB GRPC Status Codes</a>
 * @see <a href="https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/StatusCode.java">YDB Status Codes
 * and YDB Java SDK Status Codes</a>
 */
@InternalApi
public final class YdbValidator {
    private static final Logger log = LoggerFactory.getLogger(YdbValidator.class);

    private YdbValidator() {
    }

    public static void validate(String request, Status status, String response) {
        StatusCode statusCode = status.getCode();
        Issue[] issues = status.getIssues();

        switch (statusCode) {
            // Success. Do nothing ;-)
            case SUCCESS -> {
            }

            // Current session can no longer be used. Retry immediately by creating a new session
            case BAD_SESSION,      // This session is no longer available. Create a new session
                 SESSION_EXPIRED,  // The session has already expired. Create a new session
                 NOT_FOUND,        // Prepared statement or tx was not found in current session. Create a new session
                 SESSION_BUSY -> { // Another query is being executed in this session. Create a new session
                throw new BadSessionException(statusCode, request, response);
            }

            // Transaction locks invalidated.
            // Retry immediately
            case ABORTED -> throw new OptimisticLockException(response);

            // The request was cancelled **on the server** because the request timeout (CancelAfter) has expired.
            // Non-retryable
            case CANCELLED -> throw new QueryCancelledException(response);

            // YDB SDK's GRPC client's deadline expired **before** the request was sent to the server:
            // - If there is an expired GRPC context deadline **for the current thread**, assume we're running inside
            //   a GRPC request handler
            //   => No retry, terminate immediately with DeadlineExceededException to answer GRPC request promptly.
            // - If three is no GRPC context deadline for the current thread, but we timed out in session manager
            //   => Retry with slow backoff, hoping we acquire a session soon.
            // - Otherwise, assume this was an internal YOJ deadline (see YdbOperations.safeJoin())
            //   => Fail immediately with UnavailableException.
            case CLIENT_DEADLINE_EXPIRED -> {
                checkGrpcTimeoutAndCancellation(response, null);

                if (REQUEST_CREATE_SESSION.equals(request)) {
                    log.warn("""
                            Timed out waiting to get a session from the pool
                            Request: {}
                            Response: {}""", request, response);
                    throw new YdbSessionNotAcquiredException(statusCode, request, response);
                } else {
                    throw new UnavailableException(response);
                }
            }

            // DB overloaded and similar conditions. Slow retry with exponential backoff
            case OVERLOADED,                    // A part of the system is overloaded, retry and reduce the query rate.
                 CLIENT_RESOURCE_EXHAUSTED -> { // Not enough resources to process the request
                checkGrpcTimeoutAndCancellation(response, null);

                log.warn("""
                        {} | Database overloaded
                        Request: {}
                        Response: {}""", statusCode, request, response);
                throw new YdbOverloadedException(statusCode, request, response);
            }

            // The query cannot be executed in the current state. Non-retryable
            // - Primary key/UNIQUE index violations are checked first; for these we throw EntityAlreadyExistsException.
            // - All other "failed preconditions" unknown to YOJ are considered to be non-retryable.
            case PRECONDITION_FAILED -> {
                if (is(issues, IssueCode.CONSTRAINT_VIOLATION::matches)) {
                    throw new EntityAlreadyExistsException("Entity already exists" + errorMessageFrom(issues));
                } else {
                    throw new YdbPreconditionFailedException(request, response);
                }
            }

            // DB, one of its components, or the transport is temporarily unavailable. Fast retry with fixed interval
            case UNAVAILABLE,               // DB responded that it or some of its subsystems are unavailable
                 CLIENT_DISCOVERY_FAILED,   // Error occurred while retrieving the list of endpoints
                 CLIENT_LIMITS_REACHED -> { // Client-side session limit reached
                checkGrpcTimeoutAndCancellation(response, null);

                log.warn("""
                        {} | Some database components are not available, but we still got a reply from the DB
                        Request: {}
                        Response: {}""", statusCode, request, response);
                throw new YdbComponentUnavailableException(statusCode, request, response);
            }

            // The result of the request is unknown (it might have been done, it might have not). Conditionally retry
            case TIMEOUT,                   // Query timeout expired. If the query is conditionally retryable, retry it
                 UNDETERMINED,              // Indeterminate transaction state: we don't know if it has been committed
                 CLIENT_CANCELLED,          // GRPC call to the DB has been cancelled, but the DB might have done it
                 TRANSPORT_UNAVAILABLE,     // Network connectivity issues. We don't know if the DB has done the call
                 CLIENT_DEADLINE_EXCEEDED,  // YDB SDK could not get GRPC response from the DB in time
                 CLIENT_INTERNAL_ERROR -> { // Internal YDB SDK error, assumed to be transient
                checkGrpcTimeoutAndCancellation(response, null);

                log.warn("""
                        {} | Indeterminate request state: it's not known if the request succeeded or failed
                        Request: {}
                        Response: {}""", statusCode, request, response);
                throw new YdbConditionallyRetryableException(statusCode, request, response);
            }

            // GRPC client reports that the request was not authenticated properly. Retry immediately.
            // This is an internal error, but there may have been an issue with issuing the token, so we retry.
            // If that doesn’t work, we will propagate the error quickly enough.
            case CLIENT_UNAUTHENTICATED -> {
                log.warn("""
                        DB authentication failed
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbUnauthenticatedException(statusCode, request, response);
            }

            // DB reports that the request is unauthorized. Retry immediately.
            // Retries might help in case e.g. access permissions are updated in an eventually-consistent way.
            case UNAUTHORIZED -> {
                log.warn("""
                        Access to database denied
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbUnauthorizedException(statusCode, request, response);
            }

            // Database schema error, e.g. trying to access a non-existent table, column etc. No retries
            case SCHEME_ERROR -> throw new GenericSchemaException(Strings.join("\n", "Schema error", request, response));

            // Serious internal error. No retries
            case CLIENT_CALL_UNIMPLEMENTED,
                 CLIENT_GRPC_ERROR,
                 BAD_REQUEST,
                 UNSUPPORTED,
                 INTERNAL_ERROR,
                 GENERIC_ERROR,
                 UNUSED_STATUS,
                 ALREADY_EXISTS -> { // Not used by {Table,Query}Service, we consider it fatal
                log.error("""
                        {} | Bad YDB response status
                        Request: {}
                        Response: {}""", statusCode, request, response);
                throw new YdbInternalException(statusCode, request, response);
            }

            // Unknown YDB status code. No retries
            default -> {
                log.error("""
                        {} | Unknown YDB status, treating as non-retryable
                        Request: {}
                        Response: {}""", statusCode, request, response);
                throw new UnexpectedException(statusCode, request, response);
            }
        }
    }

    public static boolean isTransactionClosedByServer(Status status) {
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

    /**
     * Checks whether the current GRPC request context has an expired deadline or has been cancelled; if so, then
     * no retries will be performed for even a retryable condition, and a {@link DeadlineExceededException}
     * or a {@link QueryCancelledException} will be thrown immediately.
     *
     * @param errorMessage error message
     * @param cause        the underlying exception, if any
     */
    public static void checkGrpcTimeoutAndCancellation(String errorMessage, @Nullable Throwable cause) {
        Context ctx = Context.current();
        if (ctx.getDeadline() != null && ctx.getDeadline().isExpired()) {
            // GRPC deadline for the current GRPC context has expired. We need to throw a separate exception to avoid retries
            throw new DeadlineExceededException("DB query deadline exceeded. Response from DB: " + errorMessage, cause);
        } else if (ctx.isCancelled()) {
            // Client has cancelled the GRPC request. Throw a separate exception to avoid retries
            throw new QueryCancelledException("DB query cancelled. Response from DB: " + errorMessage);
        }
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
        CONSTRAINT_VIOLATION(2012);

        private final int issueCode;

        public boolean matches(Issue msg) {
            return msg.getCode() == issueCode;
        }
    }
}
