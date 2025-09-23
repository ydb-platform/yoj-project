package tech.ydb.yoj.repository.ydb.client;

import io.grpc.Context;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.Issue;
import tech.ydb.core.StatusCode;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.GenericSchemaException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;
import tech.ydb.yoj.repository.ydb.exception.YdbClientInternalException;
import tech.ydb.yoj.repository.ydb.exception.YdbComponentUnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbOverloadedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthenticatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthorizedException;
import tech.ydb.yoj.util.lang.Strings;

import javax.annotation.Nullable;
import java.util.function.Function;

import static lombok.AccessLevel.PRIVATE;

@InternalApi
public final class YdbValidator {
    private static final Logger log = LoggerFactory.getLogger(YdbValidator.class);

    private YdbValidator() {
    }

    public static void validate(String request, StatusCode statusCode, String response) {
        switch (statusCode) {
            // Success. Do nothing ;-)
            case SUCCESS -> {
            }

            // Current session can no longer be used. Retry immediately by creating a new session
            case BAD_SESSION,
                 SESSION_EXPIRED,
                 // Prepared statement or transaction was not found
                 NOT_FOUND -> throw new BadSessionException(response);

            // Transaction locks invalidated: somebody touched the same rows that we've read and/or changed in a SERIALIZABLE-level transaction.
            // Retry immediately
            case ABORTED -> throw new OptimisticLockException(response);

            // DB overloaded and similar conditions. Slow retry with exponential backoff
            case OVERLOADED,
                 // DB took too long to respond
                 TIMEOUT,
                 // The request was cancelled because the request timeout (CancelAfter) has expired. The request has been cancelled on the server
                 CANCELLED,
                 // Not enough resources to process the request
                 CLIENT_RESOURCE_EXHAUSTED,
                 // Deadline expired before the request was sent to the server
                 CLIENT_DEADLINE_EXPIRED,
                 // The request was cancelled on the client, at the transport level (because the GRPC deadline expired)
                 CLIENT_DEADLINE_EXCEEDED -> {
                checkGrpcContextStatus(response, null);

                // The result of the request is unknown; it might have been cancelled... or it executed successfully!
                log.warn("""
                        Database is overloaded, but we still got a reply from the DB
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbOverloadedException(request, response);
            }

            // Unknown error on the client side (most often at the transport level). Fast retry with fixed interval
            case CLIENT_CANCELLED,
                 CLIENT_GRPC_ERROR,
                 CLIENT_INTERNAL_ERROR -> {
                checkGrpcContextStatus(response, null);

                log.warn("""
                        YDB SDK internal error or cancellation
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbClientInternalException(request, response);
            }

            // DB, one of its components, or the transport is temporarily unavailable. Fast retry with fixed interval
            case UNAVAILABLE,               // DB responded that it or some of its subsystems are unavailable
                 TRANSPORT_UNAVAILABLE,     // Network connectivity issues
                 CLIENT_DISCOVERY_FAILED,   // Error occurred while retrieving the list of endpoints
                 CLIENT_LIMITS_REACHED,     // Client-side session limit reached
                 UNDETERMINED,
                 SESSION_BUSY,              // Another query is being executed in this session, should retry with a new session
                 PRECONDITION_FAILED -> {
                log.warn("""
                        Some database components are not available, but we still got a reply from the DB
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbComponentUnavailableException(request, response);
            }

            // GRPC client reports that the request was not authenticated properly. Retry immediately.
            // This is an internal error, but there may have been an issue with issuing the token, so retry can be attempted.
            // If that doesn’t work, we will propagate the error quickly enough.
            case CLIENT_UNAUTHENTICATED -> {
                log.warn("""
                        Unauthenticated request to the database
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbUnauthenticatedException(request, response);
            }

            // DB reports that the request is unauthorized. Retry immediately.
            // Retries might help in case e.g. access permissions are updated in an eventually-consistent way
            case UNAUTHORIZED -> {
                log.warn("""
                        Unauthorized request to the database
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbUnauthorizedException(request, response);
            }

            // Database schema problem, e.g. trying to access a non-existent table, column etc. No retries
            case SCHEME_ERROR -> throw new GenericSchemaException(Strings.join("\n", "Schema error", request, response));

            // Serious internal error. No retries
            case CLIENT_CALL_UNIMPLEMENTED,
                 BAD_REQUEST,
                 UNSUPPORTED,
                 INTERNAL_ERROR,
                 GENERIC_ERROR,
                 UNUSED_STATUS,
                 // This status is used by other YDB services (not the {Table,Query}Service). This is *NOT* a form of PRECONDITION_FAILED!
                 ALREADY_EXISTS -> {
                log.error("""
                        Bad response status
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbRepositoryException(request, response);
            }

            // Unknown YDB status code. No retries
            default -> {
                log.error("""
                        Unknown YDB status code, treating as non-retryable
                        Request: {}
                        Response: {}""", request, response);
                throw new YdbRepositoryException(request, response);
            }
        }
    }

    public static boolean isTransactionClosedByServer(StatusCode statusCode) {
        return switch (statusCode) {
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

    public static void checkGrpcContextStatus(String errorMessage, @Nullable Throwable cause) {
        if (Context.current().getDeadline() != null && Context.current().getDeadline().isExpired()) {
            // GRPC deadline for the current GRPC context has expired. We need to throw a separate exception to avoid retries
            throw new DeadlineExceededException("DB query deadline exceeded. Response from DB: " + errorMessage, cause);
        } else if (Context.current().isCancelled()) {
            // Client has cancelled the GRPC request. Throw a separate exception to avoid retries
            throw new QueryCancelledException("DB query cancelled. Response from DB: " + errorMessage);
        }
    }

    private static boolean is(Issue[] issues, Function<Issue, Boolean> function) {
        for (Issue issue : issues) {
            if (function.apply(issue) || (issue.getIssues().length > 0 && is(issue.getIssues(), function))) {
                return true;
            }
        }
        return false;
    }

    public static void validatePkConstraint(Issue[] issues) {
        if (is(issues, IssueCode.CONSTRAINT_VIOLATION::matches)) {
            StringBuilder error = new StringBuilder();
            is(issues, m -> {
                if (!error.isEmpty()) {
                    error.append(":");
                }
                error.append(m.getMessage());
                return false;
            });
            throw new EntityAlreadyExistsException("Entity already exists: " + error);
        }
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
