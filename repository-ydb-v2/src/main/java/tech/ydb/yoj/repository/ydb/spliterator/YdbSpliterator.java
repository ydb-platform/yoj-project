package tech.ydb.yoj.repository.ydb.spliterator;

import tech.ydb.yoj.ExperimentalApi;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/42")
public final class YdbSpliterator<V> implements ClosableSpliterator<V> {
    private final YdbSpliteratorQueue<Iterator<V>> queue;
    private final int flags;

    private Iterator<V> valueIterator;

    private boolean closed = false;

    public YdbSpliterator(YdbSpliteratorQueue<Iterator<V>> queue, boolean isOrdered) {
        this.queue = queue;
        this.flags = (isOrdered ? ORDERED : 0) | NONNULL;
    }

    // Correct way to create stream with YdbSpliterator. onClose call is important for avoid supplier thread leak.
    public Stream<V> createStream() {
        return StreamSupport.stream(this, false).onClose(this::close);
    }

    @Override
    public boolean tryAdvance(Consumer<? super V> action) {
        if (closed) {
            return false;
        }

        if (valueIterator == null || !valueIterator.hasNext()) {
            valueIterator = queue.poll();
            if (valueIterator == null || !valueIterator.hasNext()) {
                closed = true;
                return false;
            }
        }

        V value = valueIterator.next();

        action.accept(value);

        return true;
    }

    @Override
    public void close() {
        closed = true;
        queue.close();
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
