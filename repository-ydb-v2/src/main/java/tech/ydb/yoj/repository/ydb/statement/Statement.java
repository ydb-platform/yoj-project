package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction;

import java.util.List;
import java.util.Map;

/**
 * Represents a statement that can be executed in a {@link YdbRepositoryTransaction}.
 *
 * @param <PARAMS> type holding the statement's parameters
 * @param <RESULT> statement result type
 */
public interface Statement<PARAMS, RESULT> {
    // YQL

    /**
     * Tells whether the statement should be <em>prepared</em>, that is, parsed once and then cached for subsequent
     * queries during the same session.<br>
     * Prepared statements offer better query performance, but consume additional memory.
     * <p>
     * You should not blindly prepare all statements, especially non-parameterized ones; so by default this method
     * returns {@code false}.
     *
     * @return {@code true} if the statement should be prepared; {@code false} otherwise
     */
    default boolean isPreparable() {
        return false;
    }

    /**
     * Returns parameterized YQL for this query.
     *
     * @param tablespace base path for all tables referenced in the query
     * @return YQL
     */
    String getQuery(String tablespace);

    /**
     * Returns debug representation of this query with the specified parameter values.
     *
     * @param params parameter values.
     *               Might be {@code null} depending on the statement type, e.g. for DELETE statements.
     * @return debug representation of the query parameterized with {@code params}
     */
    String toDebugString(PARAMS params);

    // Parameters

    /**
     * Returns the query's parameter values as YDB protobuf structures.
     *
     * @param params parameter values
     *               Might be {@code null} depending on the statement type, e.g. for DELETE statements.
     * @return map: parameter name -> value as protobuf
     */
    Map<String, ValueProtos.TypedValue> toQueryParameters(PARAMS params);

    // Results

    /**
     * Converts YDB result set into the query result.
     *
     * @param columns result set as a YDB protobuf structure
     * @param value   result set as a YDB protobuf structure
     * @return query result
     */
    RESULT readResult(List<ValueProtos.Column> columns, ValueProtos.Value value);

    // First level cache

    /**
     * Tries to read the query result from statement cache.
     *
     * @param params parameter values.
     *               Might be {@code null} depending on the statement type, e.g. for DELETE statements.
     * @param cache  statement cache
     * @return query result, if present in statement cache; {@code null} otherwise
     *
     * @see RepositoryCache
     */
    default List<RESULT> readFromCache(PARAMS params, RepositoryCache cache) {
        return null;
    }

    /**
     * Writes the query result to statement cache.
     *
     * @param params parameter values
     *               Might be {@code null} depending on the statement type, e.g. for DELETE statements.
     * @param result result to save; if {@code null}, nothing will be saved to cache
     * @param cache  statement cache
     *
     * @see RepositoryCache
     */
    default void storeToCache(PARAMS params, List<RESULT> result, RepositoryCache cache) {
    }

    // Query Merging

    /**
     * Returns query type (for query merging purposes).
     *
     * @return query type
     */
    QueryType getQueryType();

    enum QueryType {
        UNTYPED,
        SELECT,
        INSERT,
        UPSERT,
        UPDATE,
        DELETE,
        DELETE_ALL
    }
}
