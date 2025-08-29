package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.ExperimentalApi;

/**
 * Specified how to retry YOJ tx on a {@link tech.ydb.yoj.repository.db.exception.ConditionallyRetryableException conditionally-retryable} error.
 * <p>The YOJ default is {@link #UNTIL_COMMIT}: the whole transaction body will be retried if a commit has not yet been attempted, or read-only
 * or scan mode is used.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public enum ConditionalRetryMode {
    /**
     * Never retry conditionally-retryable errors, even if the transaction commit has not yet been attempted.
     */
    NEVER,
    /**
     * Retry the whole transaction body on a conditionally-retryable error, but only if transaction commit has not yet been attempted,
     * or read-only or scan mode is used.
     */
    UNTIL_COMMIT,
    /**
     * Retry the whole transaction body on a conditionally-retryable error, even if it occurred on a transaction commit.
     */
    ALWAYS,
}
