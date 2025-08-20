package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;

import javax.annotation.Nullable;

/**
 * <strong>Experimental API:</strong> Filters which queries will be traced (=logged at {@code TRACE} level into YOJ logs), and which won't.
 * <p>Without a filter, all statements are logged at {@code TRACE} log level (but are immediately thrown away by the logging library,
 * in most cases, because in production environments YOJ loggers will be set to a {@code DEBUG} level <em>at most</em>.)
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/162")
public interface QueryTracingFilter {
    /**
     * Determines whether the query will be traced.
     *
     * @param txName {@link Tx#getName() transaction name}
     * @param txOptions transaction options, such as isolation level
     * @param queryType query type
     * @param thrown exception thrown if the query failed; {@code null} otherwise
     * @return {@code true} if the query should be traced; {@code false} otherwise
     */
    boolean shouldTrace(@NonNull String txName, @NonNull TxOptions txOptions, @NonNull QueryType queryType, @Nullable Throwable thrown);

    /**
     * Enables tracing for every query.
     */
    QueryTracingFilter ENABLE_ALL = (_1, _2, _3, _4) -> true;

    /**
     * Disables query tracing altogether.
     */
    QueryTracingFilter DISABLE_ALL = (_1, _2, _3, _4) -> false;
}
