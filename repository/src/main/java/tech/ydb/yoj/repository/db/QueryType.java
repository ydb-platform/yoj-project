package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.ExperimentalApi;

/**
 * <strong>Experimental API:</strong> Type of a single query to the database, used with {@link QueryTracingFilter}.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/162")
public enum QueryType {
    /**
     * Generic query type: not fitting well with any specific query types in {@code QueryType}.
     */
    GENERIC,
    /**
     * Table reads: {@code Table.find*()}, {@code Table.count*()}.
     * <p>Note: {@code Table.stream*()} does multiple {@code FIND} queries (it reads data in batches of {@code batchSize});
     * and {@code Table.readTable*()} is not a query but a special request to YDB, so it isn't currently traced.
     */
    FIND,
    /**
     * Exclusive inserts: {@code Table.insert()}, {@code Table.insertAll()}.
     */
    INSERT,
    /**
     * Upserts: {@code Table.save()}.
     * <p>Note: {@code Table.modifyIfPresent()}, {@code Table.saveOrUpdate()}, {@code Table.saveNewOrThrow()}, {@code Table.updateExistingOrThrow()}
     * actually produce two queries: a {@code FIND} and a {@code SAVE}.
     */
    SAVE,
    /**
     * Partial updates: {@code Table.update()}.
     */
    UPDATE,
    /**
     * {@code Table.delete()}, {@code Table.deleteAll()}.
     * <p>Note: {@code Table.deleteIfExists()} actually produces two queries: a {@code FIND} and a {@code DELETE}).
     */
    DELETE;

    /**
     * @return {@code true} if this is a read query; {@code false} otherwise
     */
    public boolean isRead() {
        return this == FIND;
    }

    /**
     * @return {@code true} if this is a write query; {@code false} otherwise
     */
    public boolean isWrite() {
        return this != FIND && this != GENERIC;
    }

    /**
     * @return {@code true} if this is a generic query type, not fitting any of the standard query types; {@code false} otherwise
     */
    public boolean isGeneric() {
        return this == GENERIC;
    }
}
