package tech.ydb.yoj.repository.ydb.spliterator;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import lombok.Getter;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/42")
public final class YdbSpliteratorQueue<V> {
    private final ArrayBlockingQueue<Supplier<V>> queue;
    private final long streamWorkDeadlineNanos;

    @Getter
    private volatile boolean closed = false;

    public YdbSpliteratorQueue(int maxQueueSize, Duration streamWorkTimeout) {
        Preconditions.checkArgument(maxQueueSize > 0, "maxQueueSize must be greater than 0");
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        this.streamWorkDeadlineNanos = System.nanoTime() + TimeUnit.NANOSECONDS.toNanos(saturatedToNanos(streamWorkTimeout));
    }

    // (grpc thread) Send values to user-stream.
    public boolean offer(V value) {
        return offerValueSupplier(() -> value);
    }

    // (grpc thread) Send knowledge to user-stream when data is over (or error handled).
    public void supplierDone(Runnable status) {
        offerValueSupplier(() -> {
            status.run();
            return null;
        });
    }

    private boolean offerValueSupplier(Supplier<V> valueSupplier) {
        if (closed) {
            return false;
        }

        try {
            if (!queue.offer(valueSupplier, calculateTimeout(), TimeUnit.NANOSECONDS)) {
                throw new OfferDeadlineExceededException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Supplier thread interrupted", e);
        }

        return !closed;
    }

    // (user thread) Get values from grpc-stream. Could be called only from one thread because of volatile closed variable
    @Nullable
    public V poll() {
        if (closed) {
            return null;
        }

        Supplier<V> valueSupplier = pollValueSupplier();
        if (valueSupplier == null) {
            throw new DeadlineExceededException("Stream deadline exceeded on poll");
        }

        V value = valueSupplier.get();
        if (value == null) {
            close();
        }

        return value;
    }

    @Nullable
    private Supplier<V> pollValueSupplier() {
        try {
            return queue.poll(calculateTimeout(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Consumer thread interrupted", e);
        }
    }

    // (user thread) Could be called only from one thread with poll() because of volatile closed variable
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        queue.clear();
    }
    
    private long calculateTimeout() {
        return TimeUnit.NANOSECONDS.toNanos(streamWorkDeadlineNanos - System.nanoTime());
    }

    // copy-paste from com.google.common.util.concurrent.Uninterruptibles
    private static long saturatedToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignore) {
            return duration.isNegative() ? -9223372036854775808L : 9223372036854775807L;
        }
    }

    public static final class OfferDeadlineExceededException extends RuntimeException {
    }
}
