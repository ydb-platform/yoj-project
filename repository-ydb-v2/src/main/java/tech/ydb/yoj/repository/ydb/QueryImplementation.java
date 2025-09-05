package tech.ydb.yoj.repository.ydb;

/**
 * Query implementation to use for this {@code YdbRepository}.
 */
public sealed interface QueryImplementation permits QueryImplementation.TableService, QueryImplementation.QueryService {
    /**
     * Use YDB {@code TableService} to perform queries. It imposes strict limits on result set size (throwing
     * {@link tech.ydb.yoj.repository.ydb.exception.ResultTruncatedException} if limits are exceeded),
     * and has less advanced session pool management, which may lead to lots of stale sessions in the event of YDB dynamic node loss.
     * <p>Maximum result set size allowed depends on your YDB configuration; typically it is 10_000 rows for production environments,
     * and 1_000 rows by default. When running YDB locally in a Docker container, you can adjust this limit by setting the
     * {@code YDB_KQP_RESULT_ROWS_LIMIT} environment variable for the container.
     * <p>This is the default query implementation in YOJ 2.x series; it will become opt-in in YOJ 3.0.0.
     */
    final class TableService implements QueryImplementation {
        // Service-specific configuration will go here in the future
    }

    /**
     * Use YDB {@code QueryService} to perform queries. This uses GRPC streaming internally, allowing for unlimited result set size
     * (but YOJ buffers the results in memory, so use an explicit {@code LIMIT} in your queries, please!).
     * {@code QueryService} also has advanced session pool management, with individual keep-alive machinery for each session, so YDB
     * node loss is less of a problem with {@code QueryService}.
     * <p>This will become default in YOJ 3.0.0.
     */
    final class QueryService implements QueryImplementation {
        // Service-specific configuration will go here in the future
    }
}
