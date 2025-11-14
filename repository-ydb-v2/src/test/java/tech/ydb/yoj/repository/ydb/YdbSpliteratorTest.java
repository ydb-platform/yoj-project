package tech.ydb.yoj.repository.ydb;

import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.SneakyThrows;
import org.junit.Test;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class YdbSpliteratorTest {
    @Test
    public void readAllSequence() {
        ReadTableMock mock = ReadTableMock.start();

        try (Stream<Integer> stream = mock.stream()) {
            List<Integer> result = stream.toList();
            assertThat(result).hasSize(ReadTableMock.SIZE);
        }

        mock.joinSupplierThread();

        assertThat(mock.bucketSizes).hasSize(ReadTableMock.BUCKETS.size());
        assertThat(mock.selectedValuesCount).hasValue(ReadTableMock.SIZE);
    }

    @Test
    public void limitDontGetAllSequence() {
        ReadTableMock mock = ReadTableMock.start();

        List<Integer> result;
        try (Stream<Integer> stream = mock.stream()) {
            result = stream.limit(2).toList();
        }
        // FIXME: This test has an inherent race, because nothing stops the supplier thread from supplying lots of data,
        // except the inefficient capacity=1 ArrayBlockingQueue inside YdbSpliterator, and the logic that advances
        // strictly by 1 element in YdbSpliterator.tryAdvance().
        // Currently this race is **masked** if we move the assertion outside of try-with-resources, because the stream
        // is then quickly close()'d **before** the YdbSpliterator is supplied the full first bucket of data.
        assertThat(result).hasSize(2);

        mock.joinSupplierThread();

        assertThat(mock.bucketSizes).hasSize(1);
        assertThat(mock.selectedValuesCount).hasValueBetween(3, 4);
    }

    @Test
    public void closeSupplierThreadWhenCloseOfLimitedStreamWasForgotten() {
        ReadTableMock mock = ReadTableMock.start();

        // stream.close() is forgotten
        List<Integer> result = mock.stream().limit(2).toList();
        assertThat(result).hasSize(2);

        // wait deadline
        mock.joinSupplierThread();

        assertThat(mock.bucketSizes).hasSize(1);
        assertThat(mock.selectedValuesCount).hasValue(4);
    }

    @Test
    @SneakyThrows
    public void endStreamWhenSupplerOfferValue() {
        YdbSpliterator<Integer> spliterator = new YdbSpliterator<>("stream", false, Duration.ofMillis(500));

        spliterator.onNext(1);

        // wait for block on onNext(2) and close stream
        var thread = new TestingThread(() -> doAfter(100, spliterator::close));
        thread.start();

        spliterator.onNext(2);
        assertThatExceptionOfType(YdbSpliterator.ConsumerDoneException.class).isThrownBy(() ->
                spliterator.onNext(3)
        );

        thread.join();
    }

    @Test
    public void getErrorOnTooSlowStreamProcessing() {
        ReadTableMock mock = ReadTableMock.start(Duration.ofMillis(100));
        assertThatExceptionOfType(DeadlineExceededException.class).isThrownBy(() -> {
            try (Stream<Integer> stream = mock.stream()) {
                stream.forEach(i -> doAfter(50, Runnables.doNothing()));
            }
        });

        mock.joinSupplierThread();
    }

    @Test
    public void getInterruptErrorFromSupplier() {
        ReadTableMock mock = ReadTableMock.start();
        assertThatExceptionOfType(QueryInterruptedException.class).isThrownBy(() -> {
            try (Stream<Integer> stream = mock.stream()) {
                stream.forEach(i -> mock.interrupt());
            }
        });

        mock.joinSupplierThread();
    }

    @SneakyThrows
    public static void doAfter(int millis, Runnable runnable) {
        Thread.sleep(millis);
        runnable.run();
    }

    private static class TestingThread {
        private final Thread thread;
        private Throwable exc;
        private boolean interrupted = false;

        public TestingThread(Runnable runnable) {
            boolean currentInterrupted = Thread.interrupted();
            if (currentInterrupted) {
                Thread.currentThread().interrupt();
            }
            thread = new Thread(() -> {
                if (currentInterrupted) {
                    Thread.currentThread().interrupt();
                }

                try {
                    runnable.run();
                } catch (Throwable e) {
                    exc = e;
                } finally {
                    interrupted = Thread.interrupted();
                }
            });
        }

        public void start() {
            thread.start();
        }

        public void interrupt() {
            thread.interrupt();
        }

        @SneakyThrows
        public void join() {
            Uninterruptibles.joinUninterruptibly(thread);
            if (exc != null) {
                throw exc;
            }
        }

        public static void runAndJoin(Runnable runnable) {
            TestingThread thread = new TestingThread(runnable);

            thread.start();

            try {
                thread.join();
            } finally {
                if (thread.interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static class ReadTableMock {
        private final static List<Integer> BUCKETS = List.of(5, 10, 3);
        private final static int SIZE = BUCKETS.stream().mapToInt(Integer::intValue).sum();

        private final AtomicInteger selectedValuesCount = new AtomicInteger();

        private final YdbSpliterator<Integer> spliterator;

        private final List<Integer> bucketSizes = new ArrayList<>();

        private TestingThread supplierThread;
        private Status status = Status.SUCCESS;
        private Throwable exception = null;

        private ReadTableMock(YdbSpliterator<Integer> spliterator) {
            this.spliterator = spliterator;
        }

        public static ReadTableMock start() {
            return start(Duration.ofMillis(500));
        }

        public static ReadTableMock start(Duration timeout) {
            YdbSpliterator<Integer> spliterator = new YdbSpliterator<>("stream", false, timeout);

            ReadTableMock mock = new ReadTableMock(spliterator);
            mock.run();
            return mock;
        }

        public void run() {
            supplierThread = new TestingThread(() -> {
                try {
                    for (int bucket : BUCKETS) {
                        bucketSizes.add(0);
                        for (int i = 0; i < bucket; i++) {
                            int next = selectedValuesCount.incrementAndGet();

                            // YdbSpliterator has to work with call onNext from different Threads
                            TestingThread.runAndJoin(() -> spliterator.onNext(next));

                            int lastIndex = bucketSizes.size() - 1;
                            bucketSizes.set(lastIndex, bucketSizes.get(lastIndex) + 1);
                        }
                    }
                } catch (Throwable e) {
                    status = Status.of(StatusCode.ABORTED);
                    exception = e;
                } finally {
                    TestingThread.runAndJoin(() -> spliterator.onSupplierThreadComplete(status, exception));
                }
            });
            supplierThread.start();
        }

        public Stream<Integer> stream() {
            return spliterator.createStream();
        }

        public void interrupt() {
            supplierThread.interrupt();
        }

        @SneakyThrows
        public void joinSupplierThread() {
            supplierThread.join();
        }
    }
}
