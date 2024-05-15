package tech.ydb.yoj.repository.ydb.spliterator.queue;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public final class YojQueueImpl<V> implements YojQueue<V> {
    private final BlockingQueue<Supplier<V>> queue;
    private final long streamWorkDeadlineNanos;

    private volatile boolean closed = false;

    public static <V> YojQueueImpl<V> create(int maxQueueSize, Duration streamWorkTimeout) {
        return new YojQueueImpl<>(
                createQueue(maxQueueSize),
                calculateDeadline(streamWorkTimeout)
        );
    }

    private YojQueueImpl(BlockingQueue<Supplier<V>> queue, long streamWorkDeadlineNanos) {
        this.queue = queue;
        this.streamWorkDeadlineNanos = streamWorkDeadlineNanos;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    // (grpc thread) Send values to user-stream.
    @Override
    public boolean offer(V value) {
        return offerValueSupplier(() -> value);
    }

    // (grpc thread) Send knowledge to user-stream when data is over (or error handled).
    @Override
    public void supplierDone(Runnable status) {
        offerValueSupplier(() -> {
            status.run();
            return null;
        });
    }

    // (user thread) Get values from grpc-stream. Could be called only from one thread because of volatile closed variable
    @Nullable
    @Override
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

    // (user thread) Could be called only from one thread with poll() because of volatile closed variable
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        queue.clear();
    }

    private boolean offerValueSupplier(Supplier<V> valueSupplier) throws OfferDeadlineExceededException {
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

    @Nullable
    private Supplier<V> pollValueSupplier() {
        try {
            return queue.poll(calculateTimeout(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Consumer thread interrupted", e);
        }
    }
    
    private long calculateTimeout() {
        return TimeUnit.NANOSECONDS.toNanos(streamWorkDeadlineNanos - System.nanoTime());
    }

    private static <V> BlockingQueue<Supplier<V>> createQueue(int maxQueueSize) {
        Preconditions.checkArgument(maxQueueSize >= 0, "maxQueueSize must be greater than 0");
        if (maxQueueSize == 0) {
            return new SynchronousQueue<>();
        }
        return new ArrayBlockingQueue<>(maxQueueSize);
    }

    private static long calculateDeadline(Duration streamWorkTimeout) {
        return System.nanoTime() + TimeUnit.NANOSECONDS.toNanos(saturatedToNanos(streamWorkTimeout));
    }

    // copy-paste from com.google.common.util.concurrent.Uninterruptibles
    private static long saturatedToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignore) {
            return duration.isNegative() ? -9223372036854775808L : 9223372036854775807L;
        }
    }
}
