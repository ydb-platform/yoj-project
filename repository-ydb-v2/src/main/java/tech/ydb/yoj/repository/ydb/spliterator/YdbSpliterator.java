package tech.ydb.yoj.repository.ydb.spliterator;

import tech.ydb.yoj.repository.ydb.spliterator.queue.YojQueue;
import tech.ydb.yoj.repository.ydb.spliterator.queue.YojSpliteratorQueue;

import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class YdbSpliterator<V> implements ClosableSpliterator<V> {
    private final YojSpliteratorQueue<Iterator<V>> queue;
    private final int flags;

    private Iterator<V> valueIterator = Collections.emptyIterator();

    private boolean closed = false;

    public YdbSpliterator(YojQueue<Iterator<V>> queue, boolean isOrdered) {
        this.queue = queue;
        this.flags = (isOrdered ? ORDERED : 0) | NONNULL;
    }

    // Correct way to create stream with YdbSpliterator. onClose call is important for avoid supplier thread leak.
    public Stream<V> createStream() {
        return StreamSupport.stream(this, false).onClose(this::close);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        queue.close();
    }

    @Override
    public boolean tryAdvance(Consumer<? super V> action) {
        if (closed) {
            return false;
        }

        // queue could return empty iterator, we have to select one with elements
        while (!valueIterator.hasNext()) {
            valueIterator = queue.poll();
            if (valueIterator == null) {
                close();
                return false;
            }
        }

        V value = valueIterator.next();

        action.accept(value);

        return true;
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
    public long getExactSizeIfKnown() {
        return -1;
    }

    @Override
    public int characteristics() {
        return flags;
    }
}
