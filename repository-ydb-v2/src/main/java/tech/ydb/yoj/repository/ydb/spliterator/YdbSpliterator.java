package tech.ydb.yoj.repository.ydb.spliterator;

import tech.ydb.yoj.ExperimentalApi;

import javax.annotation.Nullable;
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

        // WARNING: At one point in time, this spliterator will store up to queue.size() + 2 blocks from YDB in memory.
        //          One block right here, one in the queue, one in the grpc thread, waiting for free space in the queue.
        //          Maximum response size in YDB - 50mb. It means that it could be up to 150mb for spliterator.
        valueIterator = getValueIterator(valueIterator, queue);
        if (valueIterator == null) {
            close();
            return false;
        }

        V value = valueIterator.next();

        action.accept(value);

        return true;
    }

    /*
     * Returns not empty valueIterator, null in case of end of stream
     */
    @Nullable
    private static <V> Iterator<V> getValueIterator(
            @Nullable Iterator<V> valueIterator, YdbSpliteratorQueue<Iterator<V>> queue
    ) {
        // valueIterator could be null only on first call of tryAdvance
        if (valueIterator == null) {
            valueIterator = queue.poll();
            if (valueIterator == null) {
                return null;
            }
        }

        // queue could return empty iterator, we have to select one with elements
        while (!valueIterator.hasNext()) {
            valueIterator = queue.poll();
            if (valueIterator == null) {
                return null;
            }
        }

        return valueIterator;
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
