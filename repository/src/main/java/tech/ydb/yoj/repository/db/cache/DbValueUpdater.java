package tech.ydb.yoj.repository.db.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.repository.db.exception.QueryCancelledException;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.util.lang.Interrupts;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Abstract base class for caching rarely updated values, e.g. feature flags.
 * <p>Each {@link #start() active} updater runs a periodic task which retrieves the value and caches it.
 * <p>Clients should call {@link #readCached()} to retrieve value cached by an {@link #start() active}
 * updater; implementors should override {@link #doReadValue()} and choose which {@code super} constructor to call.
 * <p>Lifecycle of {@code ValueUpdater} instances should be preferably managed by some external framework, e.g.,
 * Spring lifecycle. We recommend that you call {@link #start()} at application start/component test start and
 * {@link #shutdown()} at graceful shutdown/component test shutdown.
 *
 * @see #start()
 * @see #shutdown()
 * @see #readCached()
 * @see #doReadValue()
 */
public abstract class DbValueUpdater<V> {
    protected static final Logger log = LoggerFactory.getLogger(DbValueUpdater.class);

    protected static final ThreadFactoryCreator DEFAULT_THREAD_FACTORY_CREATOR = name -> new ThreadFactoryBuilder()
            .setNameFormat(name + "-update-thread-%d")
            .setDaemon(true)
            .build();

    protected static final Duration DEFAULT_CACHE_TIMEOUT = Duration.ofSeconds(30);
    protected static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(1);
    protected static final Duration DEFAULT_MAX_LAG = Duration.ofMinutes(5);
    protected static final Duration DEFAULT_MAX_READ_DURATION = Duration.ofSeconds(15);
    protected final Duration pollInterval;
    protected final Duration shutdownTimeout;
    protected final Duration maxAge;
    protected final Duration maxReadDuration;
    protected final ThreadFactory threadFactory;
    protected final String name;
    private ScheduledExecutorService executor;
    private volatile CachedValue<V> cachedValue;

    public DbValueUpdater() {
        this(DEFAULT_THREAD_FACTORY_CREATOR);
    }

    public DbValueUpdater(@NonNull ThreadFactory threadFactory) {
        this((ThreadFactoryCreator) __ -> threadFactory);
    }

    public DbValueUpdater(@NonNull Duration pollInterval, @NonNull Duration shutdownTimeout,
                          @NonNull Duration maxAge, @NonNull Duration maxReadDuration) {
        this(pollInterval, shutdownTimeout, maxAge, maxReadDuration, DEFAULT_THREAD_FACTORY_CREATOR);
    }

    public DbValueUpdater(@NonNull String name,
                          @NonNull Duration pollInterval, @NonNull Duration shutdownTimeout,
                          @NonNull Duration maxAge, @NonNull Duration maxReadDuration) {
        this(name, pollInterval, shutdownTimeout, maxAge, maxReadDuration, DEFAULT_THREAD_FACTORY_CREATOR);
    }

    public DbValueUpdater(@NonNull ThreadFactoryCreator threadFactorySupplier) {
        this(DEFAULT_CACHE_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_MAX_LAG, DEFAULT_MAX_READ_DURATION, threadFactorySupplier);
    }

    /**
     * @deprecated This constructor uses reflection tricks to determine the name of the entity being updated.
     * This constructor will be removed in YOJ 2.7.0.
     */
    @Deprecated(forRemoval = true)
    public DbValueUpdater(@NonNull Duration pollInterval, @NonNull Duration shutdownTimeout,
                          @NonNull Duration maxAge, @NonNull Duration maxReadDuration,
                          @NonNull ThreadFactoryCreator threadFactorySupplier) {
        this(pollInterval, shutdownTimeout, maxAge, maxReadDuration, vu -> new TypeToken<V>(vu.getClass()) {
        }.getRawType().getSimpleName(), threadFactorySupplier);
        DeprecationWarnings.warnOnce("DbValueUpdater/TypeToken",
                "DbValueUpdater constructor without explicit `name` will be removed in YOJ 2.7.0. Please use the constructor with explicit name");
    }

    public DbValueUpdater(@NonNull String name,
                          @NonNull Duration pollInterval, @NonNull Duration shutdownTimeout,
                          @NonNull Duration maxAge, @NonNull Duration maxReadDuration,
                          @NonNull ThreadFactoryCreator threadFactorySupplier) {
        this(pollInterval, shutdownTimeout, maxAge, maxReadDuration, __ -> name, threadFactorySupplier);
    }

    public DbValueUpdater(@NonNull Duration pollInterval,
                          @NonNull Duration shutdownTimeout,
                          @NonNull Duration maxAge,
                          @NonNull Duration maxReadDuration,
                          @NonNull Function<DbValueUpdater<V>, String> nameSupplier,
                          @NonNull ThreadFactoryCreator threadFactoryCreator) {
        Preconditions.checkArgument(pollInterval.compareTo(Duration.ZERO) >= 0, "poll interval must be >= 0");
        Preconditions.checkArgument(shutdownTimeout.compareTo(Duration.ZERO) >= 0, "shutdown timeout must be >= 0");
        Preconditions.checkArgument(maxAge.compareTo(Duration.ZERO) > 0, "max age must be > 0");
        Preconditions.checkArgument(maxReadDuration.compareTo(Duration.ZERO) > 0, "max read duration must be > 0");

        this.pollInterval = pollInterval;
        this.shutdownTimeout = shutdownTimeout;
        this.maxAge = maxAge;
        this.maxReadDuration = maxReadDuration;
        this.name = nameSupplier.apply(this);
        this.threadFactory = threadFactoryCreator.createThreadFactory(this.name);
    }

    /**
     * @return value to cache; must not be {@code null}
     */
    @NonNull
    protected abstract V doReadValue();

    public synchronized void start() {
        if (this.executor != null) {
            return;
        }

        executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        try {
            Future<?> initialUpdate = executor.submit(this::update);
            Preconditions.checkState(null != initialUpdate.get(maxReadDuration.toMillis(), MILLISECONDS),
                    "Initial update of ValueUpdater[" + name + "] must complete successfully");

            long pollIntervalMs = pollInterval.toMillis();
            executor.scheduleWithFixedDelay(this::update, pollIntervalMs, pollIntervalMs, MILLISECONDS);
        } catch (InterruptedException | RejectedExecutionException | CancellationException e) {
            rollback(log::warn, "ValueUpdater[" + name + "] start was cancelled", e);
        } catch (TimeoutException e) {
            rollback(log::error, "Initial update for ValueUpdater[" + name + "] did not complete in " + maxReadDuration, e);
        } catch (Exception e) {
            Throwable rootCause = e instanceof ExecutionException ? e.getCause() : e;
            rollback(log::error, "Could not start ValueUpdater[" + name + "]", rootCause);
        }
    }

    private void rollback(TriConsumer<String, String, Throwable> logMethod, String message, Throwable cause) {
        logMethod.accept("{}; shutting down", message, cause);

        IllegalStateException ex = new IllegalStateException(message, cause);
        try {
            shutdown();
        } catch (Exception shutdownEx) {
            ex.addSuppressed(shutdownEx);
        }
        throw ex;
    }

    public synchronized void shutdown() {
        if (executor == null) {
            return;
        }

        executor.shutdownNow();
        Preconditions.checkState(
                Interrupts.awaitTermination(executor, shutdownTimeout),
                "Could not stop ValueUpdater[%s] in %s", name, shutdownTimeout
        );
        executor = null;

        cachedValue = null;
    }

    /**
     * Checks if updater is active
     *
     * @return true if updater is active (cachedValue not null), else false
     */
    public synchronized boolean isUpdaterActive() {
        return cachedValue != null;
    }

    /**
     * @return cached value
     * @throws IllegalStateException if value updater is not active
     */
    @NonNull
    public V readCached() {
        CachedValue<V> cv = cachedValue;
        Preconditions.checkState(cv != null, "Value updater is not active");
        return cv.value;
    }

    /**
     * Forces an immediate value update. Will throw if the value cannot be updated.
     * <br><strong>This method should ONLY be used in tests.</strong>
     */
    @VisibleForTesting
    public void forceUpdate() {
        this.cachedValue = new CachedValue<>(doReadValue(), Instant.now());
    }

    @Override
    public String toString() {
        return "ValueUpdater[" + name + "]=" + cachedValue;
    }

    private V update() {
        V newValue = tryReadValue();
        Instant now = Instant.now();

        CachedValue<V> prevCached = this.cachedValue;
        Instant lastGoodPoll = prevCached == null ? null : prevCached.lastGoodPoll;
        Duration age = lastGoodPoll == null ? Duration.ZERO : Duration.between(lastGoodPoll, now);
        logErrorIf(age.compareTo(maxAge) > 0, () -> format("[%s] Cached value is too old: %s > %s", name, age, maxAge));
        logErrorIf(newValue == null && lastGoodPoll == null, () -> format("[%s] No read value available AND no cached value present", name));

        if (newValue != null) {
            this.cachedValue = new CachedValue<>(newValue, now);
        }
        return newValue;
    }

    @Nullable
    protected V tryReadValue() {
        Instant started = Instant.now();
        try {
            return doReadValue();
        } catch (QueryInterruptedException | QueryCancelledException e) {
            log.info("[{}] Cancelled/interrupted while trying to read value", name, e);
            return null;
        } catch (Exception e) {
            log.warn("[{}] Could not read value: {}", name, e.getClass().getSimpleName(), e);
            return null;
        } finally {
            Instant finished = Instant.now();
            Duration readDuration = Duration.between(started, finished);
            if (readDuration.compareTo(maxReadDuration) > 0) {
                log.error("[{}] readValue() took too long: {} > {}", name, readDuration, maxReadDuration);
            }
        }
    }

    private static void logErrorIf(boolean errorCondition, Supplier<String> message) {
        if (errorCondition) {
            log.error(message.get());
        }
    }

    @FunctionalInterface
    public interface ThreadFactoryCreator {
        ThreadFactory createThreadFactory(String valueUpdaterName);
    }

    @FunctionalInterface
    private interface TriConsumer<A, B, C> {
        void accept(A a, B b, C c);
    }

    @Value
    private static class CachedValue<V> {
        @NonNull
        V value;

        @NonNull
        Instant lastGoodPoll;

        @NonNull
        @Override
        public String toString() {
            return value.toString();
        }
    }
}
