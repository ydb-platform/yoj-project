package tech.ydb.yoj.repository.db.cache;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.Test;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.fail;

public class DbValueUpdaterTest {
    @Test
    @SneakyThrows
    public void timeoutOnStart() {
        var vu = new DbValueUpdater<>(
                "timeout-on-start",
                /*       poll interval */ Duration.ofDays(1),
                /*    shutdown timeout */ Duration.ofSeconds(10),
                /* max valid value age */ Duration.ofDays(1),
                /*        read timeout */ Duration.ofMillis(100)
        ) {
            @Override
            protected @NonNull Object doReadValue() {
                return awaitInterrupt();
            }
        };

        assertThatIllegalStateException().isThrownBy(vu::start);
        assertThatIllegalStateException().isThrownBy(vu::readCached);
    }

    @Test
    @SneakyThrows
    public void errorOnStart() {
        var vu = new DbValueUpdater<>(
                "error-on-start",
                /*       poll interval */ Duration.ofDays(1),
                /*    shutdown timeout */ Duration.ofSeconds(10),
                /* max valid value age */ Duration.ofDays(1),
                /*        read timeout */ Duration.ofSeconds(10)
        ) {
            @Override
            protected @NonNull Object doReadValue() {
                if (true) {
                    throw new DeadlineExceededException("Query timed out, sorry");
                }
                return new Object(); // Will never be reached, but compiler doesn't know that
            }
        };

        assertThatIllegalStateException().isThrownBy(vu::start);
        assertThatIllegalStateException().isThrownBy(vu::readCached);
    }

    @Test
    @SneakyThrows
    public void interruptOnStart() {
        CountDownLatch readValueEnter = new CountDownLatch(1);
        var vu = new DbValueUpdater<>("interrupt-on-start",
                /*       poll interval */ Duration.ofDays(1),
                /*    shutdown timeout */ Duration.ofSeconds(10),
                /* max valid value age */ Duration.ofDays(1),
                /*        read timeout */ Duration.ofSeconds(10)
        ) {
            @Override
            protected @NonNull Object doReadValue() {
                readValueEnter.countDown();
                return awaitInterrupt();
            }
        };

        var startException = new AtomicReference<Throwable>();
        var startThread = new Thread(() -> {
            try {
                vu.start();
            } catch (Throwable t) {
                startException.set(t);
            }
        });
        startThread.start();
        readValueEnter.await();
        startThread.interrupt();
        startThread.join();

        assertThat(startException).doesNotHaveValue(null);
        assertThat(startException.get()).isInstanceOf(IllegalStateException.class);
    }

    private static Object awaitInterrupt() {
        try {
            // Just a fancy way of waiting for thread interrupt
            new CountDownLatch(1).await();
            fail("Expected interrupt to happen");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryInterruptedException("DB query interrupted", e);
        }
        return new Object(); // Will never be reached, but compiler doesn't know this
    }
}
