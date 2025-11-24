package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema;

/**
 * Result ordering mode for queries with index.
 *
 * @see EntityExpressions#orderByIndex(EntitySchema, String, IndexOrder)
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/192")
public enum IndexOrder {
    /**
     * Order query results by {@code (all index fields, then all Entity ID fields)} ascending.
     */
    ASCENDING,
    /**
     * Order query results by {@code (all index fields, then all Entity ID fields)} descending.
     * <br>This might offer worse query plans than {@link #ASCENDING} and {@link #UNORDERED}.
     */
    DESCENDING,
    /**
     * Return query results in <em>no particular order</em> (for small tables and query result sets,
     * the results might be <em>almost</em> sorted by index table's primary key ascending).
     *
     * @see tech.ydb.yoj.databind.expression.OrderExpression#unordered(Schema)
     */
    UNORDERED,
}
