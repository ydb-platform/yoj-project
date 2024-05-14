package tech.ydb.yoj.repository.ydb;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @deprecated Legacy implementation of {@code Spliterator} for {@code ReadTable}. Will be eventually removed in a future YOJ version.
 * <p>To use the new {@code Spliterator} contract-conformant implementation, set {@code ReadTableParams.builder().<...>.useNewSpliterator(true)}.
 * <p>Note that using the new implementation currently has a negative performance impact, for more information refer to
 * <a href="https://github.com/ydb-platform/yoj-project/issues/42">GitHub Issue #42</a>.
 */
@Deprecated
public class YdbLegacySpliterator<V> implements Spliterator<V> {
    private final int flags;
    private final Consumer<Consumer<? super V>> action;

    public YdbLegacySpliterator(boolean isOrdered, Consumer<Consumer<? super V>> action) {
        this.action = action;
        flags = (isOrdered ? ORDERED : 0) | NONNULL;
    }

    public Stream<V> makeStream() {
        return StreamSupport.stream(this, false);
    }

    @Override
    public boolean tryAdvance(Consumer<? super V> action) {
        this.action.accept(action);
        return false;
    }

    @Override
    public Spliterator<V> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return flags;
    }
}
