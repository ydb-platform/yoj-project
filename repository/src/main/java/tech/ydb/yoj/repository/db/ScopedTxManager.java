package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.BaseDb;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/197")
public final class ScopedTxManager<D extends BaseDb> extends DelegatingTxManager<ScopedTxManager<D>> {
    private final D db;

    public ScopedTxManager(@NonNull Repository repository, @NonNull Class<D> dbClass) {
        this(new StdTxManager(repository), dbClass);
    }

    public ScopedTxManager(@NonNull TxManager delegate, @NonNull Class<D> dbClass) {
        this(delegate, BaseDb.current(dbClass));
    }

    private ScopedTxManager(@NonNull TxManager delegate, @NonNull D db) {
        super(delegate);
        this.db = db;
    }

    public <R> R call(@NonNull Function<D, R> function) {
        return tx(() -> function.apply(db));
    }

    public <T> T call(@NonNull BiFunction<D, Tx, T> function) {
        return tx(() -> function.apply(db, Tx.Current.get()));
    }

    public void run(@NonNull Consumer<D> consumer) {
        tx(() -> consumer.accept(db));
    }

    public void run(@NonNull BiConsumer<D, Tx> consumer) {
        tx(() -> consumer.accept(db, Tx.Current.get()));
    }

    @Override
    protected ScopedTxManager<D> createTxManager(TxManager delegate) {
        return new ScopedTxManager<>(delegate, this.db);
    }
}
