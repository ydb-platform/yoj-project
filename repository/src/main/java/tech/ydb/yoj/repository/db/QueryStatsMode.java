package tech.ydb.yoj.repository.db;

/**
 * Query statistics collection mode.
 */
public enum QueryStatsMode {
    /**
     * No query statistics are collected. <em>This is the default.</em>
     */
    NONE,
    /**
     * Basic query statistics: Aggregated stats of reads, updates and deletes per table (but no query plan).
     */
    BASIC,
    /**
     * Full query statistics: Basic query statistics, plus execution statistics, plus query plan.
     */
    FULL,
    /**
     * Full query statistics, plus detailed execution statistics, including those for individual tasks and channels.
     */
    PROFILE
}
