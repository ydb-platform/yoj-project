package tech.ydb.yoj.repository.db;

import lombok.NonNull;

import javax.annotation.Nullable;

public interface TxManagerState {
    @NonNull
    Repository getRepository();

    @Nullable
    String getLogContext();

    @Nullable
    IsolationLevel getIsolationLevel();

    boolean isReadOnly();

    boolean isScan();

    boolean isFirstLevelCache();

    boolean isDryRun();
}
