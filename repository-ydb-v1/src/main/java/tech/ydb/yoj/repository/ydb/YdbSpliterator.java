package tech.ydb.yoj.repository.ydb;

import com.google.common.annotations.VisibleForTesting;
import com.yandex.ydb.core.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.ydb.client.YdbValidator;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@code YdbSpliterator} is used for read data from YDB streams.
 * It's possible to supply values from different threads, but supplier threads mustn't call onNext concurrently.
 */
class YdbSpliterator<V> implements Spliterator<V> {
    private static final Logger log = LoggerFactory.getLogger(YdbSpliterator.class);

    private static final Duration DEFAULT_STREAM_WORK_TIMEOUT = Duration.ofMinutes(5);

    // Deadline for stream work
    private final long streamWorkDeadlineNanos;
    private final int flags;
    // ArrayBlockingQueue(1) is used instead SynchronousQueue because clear() behavior is needed
    private final BlockingQueue<QueueValue<V>> queue = new ArrayBlockingQueue<>(1);
    private final BiConsumer<Status, Throwable> validateResponse;

    private volatile boolean closed = false;

    private boolean endData = false;

    public YdbSpliterator(String request, boolean isOrdered) {
        this(request, isOrdered, DEFAULT_STREAM_WORK_TIMEOUT);
    }

    @VisibleForTesting
    protected YdbSpliterator(String request, boolean isOrdered, Duration streamWorkTimeout) {
        this.flags = (isOrdered ? ORDERED : 0) | NONNULL;
        this.streamWorkDeadlineNanos = System.nanoTime() + TimeUnit.NANOSECONDS.toNanos(saturatedToNanos(streamWorkTimeout));
        this.validateResponse = (status, error) -> {
            if (error != null) {
                throw YdbOperations.convertToRepositoryException(error);
            }
            YdbValidator.validate(request, status.getCode(), status.toString());
        };
    }

    private long calculateTimeout() {
        return TimeUnit.NANOSECONDS.toNanos(streamWorkDeadlineNanos - System.nanoTime());
    }

    // Correct way to create stream with YdbSpliterator. onClose call is important for avoid supplier thread leak.
    public Stream<V> createStream() {
        return StreamSupport.stream(this, false).onClose(this::close);
    }

    // (supplier thread) Send data to stream thread.
    public void onNext(V value) {
        if (closed) {
            // Need to abort supplier thread if stream is closed. onSupplierThreadComplete will exit immediately.
            // ConsumerDoneException isn't handled because onSupplierThreadComplete will exit by streamClosed.
            throw ConsumerDoneException.INSTANCE;
        }

        try {
            if (!queue.offer(QueueValue.of(value), calculateTimeout(), TimeUnit.NANOSECONDS)) {
                log.warn("Supplier thread was closed because consumer didn't poll an element of stream on timeout");
                throw OfferDeadlineExceededException.INSTANCE;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Supplier thread interrupted", e);
        }
    }

    // (supplier thread) Send knowledge to stream when data is over.
    public void onSupplierThreadComplete(Status status, Throwable ex) {
        ex = unwrapException(ex);
        if (ex instanceof OfferDeadlineExceededException || closed) {
            // If deadline exceeded happen, need to do nothing. Stream thread will exit at deadline by themself.
            return;
        }

        QueueValue<V> endValue = QueueValue.ofEndData(status, ex);

        // Sending endData for notify stream thread that data was ended. Should send this even if supplier thread
        // interrupted for completing stream correctly (without deadline)
        if (!offerUninterruptibly(queue, endValue, streamWorkDeadlineNanos)) {
            // If deadline exceeded need to do nothing. Stream thread will exit at deadline by themself.
            log.warn("Supplier thread was closed because consumer didn't poll the last element of stream on timeout");
        }
    }

    // (stream thread)
    @Nullable
    private QueueValue<V> poll() {
        try {
            return queue.poll(calculateTimeout(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("Consumer thread interrupted", e);
        }
    }

    @Override
    // (stream thread) called from stream engine for get new value.
    public boolean tryAdvance(Consumer<? super V> action) {
        // Stream API can call tryAdvance() once again even if tryAdvance() returned false
        if (endData) {
            return false;
        }

        QueueValue<V> value = poll();
        if (value == null) {
            throw new DeadlineExceededException("Stream deadline exceeded on poll");
        }

        if (value.endData()) {
            endData = true;
            validateResponse.accept(value.status(), value.error());
            return false;
        }

        action.accept(value.value());
        return true;
    }

    // (stream thread) close spliterator and abort supplier thread
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        // Abort offer in supplier thread. onNext() will look at streamClosed and exit immediately.
        // onSupplierThreadComplete() just will exit.
        queue.clear();
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

    private record QueueValue<V>(
            V value,
            Status status,
            Throwable error,
            boolean endData
    ) {
        public static <V> QueueValue<V> of(V value) {
            return new QueueValue<>(value, null, null, false);
        }

        public static <V> QueueValue<V> ofEndData(Status status, Throwable ex) {
            return new QueueValue<>(null, status, ex, true);
        }
    }

    private static Throwable unwrapException(Throwable ex) {
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            return ex.getCause();
        }
        return ex;
    }

    @VisibleForTesting
    protected static class ConsumerDoneException extends RuntimeException {
        public final static ConsumerDoneException INSTANCE = new ConsumerDoneException();
    }

    private static class OfferDeadlineExceededException extends RuntimeException {
        public final static OfferDeadlineExceededException INSTANCE = new OfferDeadlineExceededException();
    }

    // copy-paste from com.google.common.util.concurrent.Uninterruptibles
    private static long saturatedToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignore) {
            return duration.isNegative() ? -9223372036854775808L : 9223372036854775807L;
        }
    }

    private static <E> boolean offerUninterruptibly(BlockingQueue<E> queue, E element, long deadlineNanos) {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long timeout = TimeUnit.NANOSECONDS.toNanos(deadlineNanos - System.nanoTime());
                    return queue.offer(element, timeout, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
