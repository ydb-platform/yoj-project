package tech.ydb.yoj.repository.db.listener;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.RepositoryTransaction;

public interface RepositoryTransactionListener {
    default void onFlushWrites(@NonNull RepositoryTransaction transaction) {
    }

    default void onCommit(@NonNull RepositoryTransaction transaction) {
    }

    default void onRollback(@NonNull RepositoryTransaction transaction) {
    }
}
