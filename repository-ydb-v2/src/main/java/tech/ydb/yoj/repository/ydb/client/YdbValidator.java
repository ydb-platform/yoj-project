package tech.ydb.yoj.repository.ydb.client;

import io.grpc.Context;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.Issue;
import tech.ydb.core.StatusCode;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;
import tech.ydb.yoj.repository.ydb.exception.ResultTruncatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbClientInternalException;
import tech.ydb.yoj.repository.ydb.exception.YdbComponentUnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbOverloadedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.exception.YdbSchemaException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthenticatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbUnauthorizedException;

import javax.annotation.Nullable;
import java.util.function.Function;

import static lombok.AccessLevel.PRIVATE;

public final class YdbValidator {
    private static final Logger log = LoggerFactory.getLogger(YdbValidator.class);

    private YdbValidator() {
    }

    public static void validate(String request, StatusCode statusCode, String response) {
        switch (statusCode) {
            case SUCCESS:
                return;

            case BAD_SESSION:
            case SESSION_EXPIRED:
            case NOT_FOUND: // вероятнее всего это проблемы с prepared запросом. Стоит поретраить с новой сессией. Еще может быть Transaction not found
                throw new BadSessionException(response);

            case ABORTED:
                throw new OptimisticLockException(response);

            case OVERLOADED: // БД перегружена - нужно ретраить с экспоненциальной задержкой
            case TIMEOUT: // БВ отовечечала слишком долго - нужно ретраить с экспоненциальной задержкой
            case CANCELLED: // запрос был отменен, тк закончился установленный в запросе таймаут  (CancelAfter). Запрос на сервере гарантированно отменен.
            case CLIENT_RESOURCE_EXHAUSTED: // недостаточно свободных ресурсов для обслуживания запроса
            case CLIENT_DEADLINE_EXPIRED: // deadline expired before request was sent to server.
            case CLIENT_DEADLINE_EXCEEDED: // запрос был отменен на транспортном уровне, тк закончился установленный дедлайн.
                checkGrpcContextStatus(response, null);

                // Резльтат обработки запроса не известен - может быть отменен или выполнен.
                log.warn("Database is overloaded, but we still got a reply from the DB\n" +
                        "Request: {}\nResponse: {}", request, response);

                throw new YdbOverloadedException(request, response);

            case CLIENT_CANCELLED:
            case CLIENT_INTERNAL_ERROR: // неизвестная ошибка на клиентской стороне (чаще всего транспортного уровня)
                checkGrpcContextStatus(response, null);

                log.warn("Some database components are not available, but we still got a reply from the DB\n"
                        + "Request: {}\nResponse: {}", request, response);
                throw new YdbClientInternalException(request, response);
            case UNAVAILABLE: // БД ответила, что она или часть ее подсистем не доступны
            case TRANSPORT_UNAVAILABLE: // проблемы с сетевой связностью
            case CLIENT_DISCOVERY_FAILED: // ошибка в ходе получения списка эндпоинтов
            case CLIENT_LIMITS_REACHED: // достигнут лимит на количество сессий на клиентской стороне
            case UNDETERMINED:
            case SESSION_BUSY: // в этот сессии скорей всего исполняется другой запрос, стоит поретраить с новой сессией
            case PRECONDITION_FAILED:
                log.warn("Some database components are not available, but we still got a reply from the DB\n" +
                        "Request: {}\nResponse: {}", request, response);
                throw new YdbComponentUnavailableException(request, response);

            case CLIENT_UNAUTHENTICATED: // grpc сообщил, что запрос не аутентифицирован. Это интернал ошибка, но возможно
                // была проблема с выпиской токена и можно попробовать поретраить - если не поможет, то отдать наружу
                log.warn("Database said we are not authenticated\nRequest: {}\nResponse: {}", request, response);
                throw new YdbUnauthenticatedException(request, response);

            case UNAUTHORIZED: // БД сообщила, что запрос не авторизован. Ретраи могут помочь
                log.warn("Database said we are not authorized\nRequest: {}\nResponse: {}", request, response);
                throw new YdbUnauthorizedException(request, response);

            case SCHEME_ERROR:
                throw new YdbSchemaException("schema error", request, response);

            case CLIENT_CALL_UNIMPLEMENTED:
            case BAD_REQUEST:
            case UNSUPPORTED:
            case INTERNAL_ERROR:
            case GENERIC_ERROR:
            case UNUSED_STATUS:
            case ALREADY_EXISTS: // Этот статус используется другими ydb-сервисами. Это не вид precondition failed!
            default:
                log.error("Bad response status\nRequest: {}\nResponse: {}", request, response);
                throw new YdbRepositoryException(request, response);
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
                    CLIENT_RESOURCE_EXHAUSTED -> false;
        };
    }

    public static void checkGrpcContextStatus(String errorMessage, @Nullable Throwable cause) {
        if (Context.current().getDeadline() != null && Context.current().getDeadline().isExpired()) {
            // время на обработку запроса закончилось, нужно выбросить отдельное исключение чтобы не было ретраев
            throw new DeadlineExceededException("DB query deadline exceeded. Response from DB: " + errorMessage, cause);
        } else if (Context.current().isCancelled()) {
            // запрос отменил сам клиент, эту ошибку не нужно ретраить
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
                if (error.length() > 0) {
                    error.append(":");
                }
                error.append(m.getMessage());
                return false;
            });
            throw new EntityAlreadyExistsException("Entity already exists: " + error.toString());
        }
    }

    public static void validateTruncatedResults(String yql, DataQueryResult queryResult) {
        for (int i = 0; i < queryResult.getResultSetCount(); i++) {
            validateTruncatedResults(yql, queryResult.getResultSet(i));
        }
    }

    public static void validateTruncatedResults(String yql, ResultSetReader rs) {
        if (rs.isTruncated()) {
            String message = "Results was truncated to " + rs.getRowCount() + " elements";
            throw new ResultTruncatedException(message, yql, rs.getRowCount());
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
