package tech.ydb.yoj.util.lang;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class Interrupts {
    private Interrupts() {
    }

    /**
     * Checks whether the current thread has been interrupted or got an <em>interrupt-signaling exception</em>
     * (that is, an exception caused by {@code InterruptedException}, {@code InterruptedIOException} or one of
     * their subclasses).
     *
     * @return {@code true} if current thread has its interrupt flag set or {@code t} is interrupt-signaling;
     * {@code false} otherwise
     * @see #isThreadInterrupted()
     * @see #isInterruptException(Throwable)
     */
    public static boolean isThreadInterrupted(Throwable t) {
        return isThreadInterrupted() || isInterruptException(t);
    }

    /**
     * Checks whether t is an <em>interrupt-signaling exception</em>
     * (i.e. either is an {@code InterruptedException} or {@code InterruptedIOException}, or directly or indirectly
     * caused by one of them).
     *
     * @return {@code true} if {@code t} is interrupt-signaling; {@code false} otherwise
     * @see #isThreadInterrupted()
     */
    public static boolean isInterruptException(Throwable t) {
        if (Exceptions.isOrCausedBy(t, InterruptedException.class)) {
            return true;
        }
        if (Exceptions.isOrCausedByExact(t, InterruptedIOException.class)) {
            // InterruptedIOException has inheritors (like SocketTimeoutException) which throws without thread interrupt.
            // These exceptions should be skipped in interrupts logic.
            return true;
        }
        return Exceptions.isOrCausedBy(t, ClosedByInterruptException.class);
    }

    /**
     * Checks whether the current thread has its interrupt flag set. Does not change the interrupt flag.
     *
     * @return {@code true} if current thread has interrupt flag set; {@code false} otherwise
     * @see #isThreadInterrupted(Throwable)
     */
    public static boolean isThreadInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    public static boolean awaitTermination(ExecutorService executor, Duration timeout) {
        try {
            return executor.awaitTermination(timeout.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static void runInCleanupMode(Runnable action) {
        boolean interrupted = Thread.interrupted();
        try {
            action.run();
        } catch (Exception e) {
            interrupted = interrupted || isInterruptException(e);
            throw e;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
