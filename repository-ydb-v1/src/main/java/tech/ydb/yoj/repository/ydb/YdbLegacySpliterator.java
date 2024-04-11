package tech.ydb.yoj.repository.ydb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @deprecated Legacy implementation of {@code Spliterator} for {@code ReadTable}. Will be removed in YOJ 3.0.0.
 * <p>To opt into using of this legacy implementation, explicitly set {@code ReadTableParams.<ID>builder().<...>.useNewSpliterator(false)}.
 */
@Deprecated(forRemoval = true)
class YdbLegacySpliterator<V> implements Spliterator<V> {
    private static final Logger log = LoggerFactory.getLogger(YdbLegacySpliterator.class);

    private final int flags;
    private final Consumer<Consumer<? super V>> action;

    public YdbLegacySpliterator(boolean isOrdered, Consumer<Consumer<? super V>> action) {
        log.error("You are using YdbLegacySpliterator which is deprecated for removal in YOJ 3.0.0. "
                + "Please use readTable(ReadTableParams.builder().<...>.useNewSpliterator(true).build())",
                new Throwable("YdbLegacySpliterator construction stack trace"));
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
