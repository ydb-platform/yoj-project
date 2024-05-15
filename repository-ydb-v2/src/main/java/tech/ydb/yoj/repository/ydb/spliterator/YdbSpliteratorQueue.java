package tech.ydb.yoj.repository.ydb.spliterator;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/42")
public final class YdbSpliteratorQueue<V> {
    private static final Logger log = LoggerFactory.getLogger(YdbSpliteratorQueue.class);

    private static final SupplierStatus UNDONE_SUPPLIER_STATUS = () -> false;

    private final int maxQueueSize;
    private final ArrayDeque<V> queue;
    private final long streamWorkDeadlineNanos;

    private final Lock lock = new ReentrantLock();
    private final Condition newElement = lock.newCondition();
    private final Condition queueIsNotFull = lock.newCondition();

    private SupplierStatus supplierStatus = UNDONE_SUPPLIER_STATUS;
    private boolean closed = false;

    public YdbSpliteratorQueue(int maxQueueSize, Duration streamWorkTimeout) {
        Preconditions.checkArgument(maxQueueSize > 0, "maxQueueSize must be greater than 0");
        this.maxQueueSize = maxQueueSize;
        this.queue = new ArrayDeque<>(maxQueueSize);
        this.streamWorkDeadlineNanos = System.nanoTime() + TimeUnit.NANOSECONDS.toNanos(saturatedToNanos(streamWorkTimeout));
    }

    public boolean onNext(V value) {
        Preconditions.checkState(supplierStatus.equals(UNDONE_SUPPLIER_STATUS),
                "can't call onNext after supplierDone"
        );

        lock.lock();
        try {
            if (!awaitFreeSpaceLocked()) {
                return false;
            }

            queue.add(value);

            newElement.signal();
        } finally {
            lock.unlock();
        }

        return true;
    }

    public boolean awaitFreeSpace() {
        Preconditions.checkState(supplierStatus.equals(UNDONE_SUPPLIER_STATUS),
                "can't call onNext after supplierDone"
        );

        lock.lock();
        try {
            return awaitFreeSpaceLocked();
        } finally {
            lock.unlock();
        }
    }

    private boolean awaitFreeSpaceLocked() {
        if (closed) {
            return false;
        }

        if (queue.size() != maxQueueSize) {
            return true;
        }

        try {
            if (!queueIsNotFull.await(calculateTimeout(), TimeUnit.NANOSECONDS)) {
                throw new OfferDeadlineExceededException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Supplier thread interrupted", e);
        }

        return !closed;
    }

    // (supplier thread) Send knowledge to stream when data is over.
    public void supplierDone(SupplierStatus status) {
        lock.lock();
        try {
            if (closed) {
                return;
            }

            supplierStatus = status;

            newElement.signal();
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    public V poll() {
        lock.lock();
        try {
            if (closed) {
                return null;
            }

            if (queue.isEmpty()) {
                if (supplierStatus.isDone()) {
                    return null;
                }

                try {
                    if (!newElement.await(calculateTimeout(), TimeUnit.NANOSECONDS)) {
                        log.warn("Supplier thread was closed because consumer didn't poll an element of stream on timeout");
                        throw new DeadlineExceededException("Stream deadline exceeded on poll");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new QueryInterruptedException("Consumer thread interrupted", e);
                }

                if (closed || supplierStatus.isDone()) {
                    return null;
                }
            }

            V value = queue.pop();

            queueIsNotFull.signal();

            return value;
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }

            closed = true;

            queueIsNotFull.signal();
            newElement.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private long calculateTimeout() {
        return TimeUnit.NANOSECONDS.toNanos(streamWorkDeadlineNanos - System.nanoTime());
    }

    public static final class OfferDeadlineExceededException extends RuntimeException {
    }

    // copy-paste from com.google.common.util.concurrent.Uninterruptibles
    private static long saturatedToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignore) {
            return duration.isNegative() ? -9223372036854775808L : 9223372036854775807L;
        }
    }

    @FunctionalInterface
    public interface SupplierStatus {
        boolean isDone();
    }
}
