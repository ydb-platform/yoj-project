package tech.ydb.yoj.repository.db.listener;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

/**
 * Listener for transaction events.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
public interface RepositoryTransactionListener {
    /**
     * This event happens right before we attempt to perform delayed writes (if any) and commit the transaction.
     * This gives the last opportunity to perform more writes in the current transaction.
     * <p><em>Note:</em> Currently, this event happens in both <em>delayed writes</em> transactions (where all writes are queued until commit,
     * and can be merged together) and in <em>immediate writes</em> transactions (where all writes are executed immediately).
     * <strong>This is an implementation detail that <em>might</em> change in the future.</strong>
     *
     * @see #onImmediateWrite(RepositoryTransaction)
     *
     * @param transaction repository transaction
     */
    default void onBeforeFlushWrites(@NonNull RepositoryTransaction transaction) {
    }

    /**
     * A write query has been successfully executed in this <em>immediate writes</em> transaction. This event does <strong>not</strong> happen
     * in a <em>delayed writes</em> transaction.
     *
     * @param transaction repository transaction
     */
    default void onImmediateWrite(@NonNull RepositoryTransaction transaction) {
    }

    /**
     * Transaction has been committed successfully.
     *
     * @param transaction repository transaction
     * @see RepositoryTransaction#commit()
     */
    default void onCommit(@NonNull RepositoryTransaction transaction) {
    }

    /**
     * Transaction has been rolled back successfully.
     *
     * @param transaction repository transaction
     * @see RepositoryTransaction#rollback()
     */
    default void onRollback(@NonNull RepositoryTransaction transaction) {
    }
}
