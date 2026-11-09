package tech.ydb.yoj.prometheus;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import lombok.NonNull;
import tech.ydb.yoj.tracer.YojTracer;
import tech.ydb.yoj.util.lang.Strings;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Supplier;

/** Prometheus Java client 1.x instrumentation for YOJ transactions and session pools. */
public final class PrometheusYojTracer implements YojTracer {
    // Values do not retain their registry, so unused custom registries can be collected.
    private static final Map<PrometheusRegistry, PrometheusYojTracer> byRegistry = new WeakHashMap<>();

    private static final double[] TX_ATTEMPTS_BUCKETS = new double[]
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 35, 40, 45, 50, 60, 70, 80, 90, 100};
    static final double[] DURATION_BUCKETS = {
            .001, .0025, .005, .0075,
            .01, .025, .05, .075,
            .1, .25, .5, .75,
            1, 2.5, 5, 7.5,
            10, 25, 50, 75,
            100
    };

    private final Histogram totalDuration = Histogram.builder()
            .name("tx_total_duration_seconds")
            .help("Tx total duration (seconds)")
            .labelNames("tx_name")
            .classicOnly()
            .classicUpperBounds(DURATION_BUCKETS)
            .build();
    private final Histogram attemptDuration = Histogram.builder()
            .name("tx_attempt_duration_seconds")
            .help("Tx attempt duration (seconds)")
            .labelNames("tx_name")
            .classicOnly()
            .classicUpperBounds(DURATION_BUCKETS)
            .build();
    private final Histogram attempts = Histogram.builder()
            .name("tx_attempts")
            .help("Tx attempts spent")
            .labelNames("tx_name")
            .classicOnly()
            .classicUpperBounds(TX_ATTEMPTS_BUCKETS)
            .build();
    private final Counter results = Counter.builder()
            .name("tx_result")
            .help("Tx commits/rollbacks/fails")
            .labelNames("tx_name", "result")
            .build();
    private final Counter retries = Counter.builder()
            .name("tx_retries")
            .help("Tx retry reasons")
            .labelNames("tx_name", "reason")
            .build();

    private final SessionMetrics sessions;

    private PrometheusYojTracer(PrometheusRegistry registry) {
        registry.register(totalDuration);
        registry.register(attemptDuration);
        registry.register(attempts);
        registry.register(results);
        registry.register(retries);
        sessions = new SessionMetrics(registry);
    }

    /**
     * Returns the shared tracer for a registry, registering YOJ metrics only once.
     * Pass {@link PrometheusRegistry#defaultRegistry} explicitly to use the global registry.
     */
    public static synchronized PrometheusYojTracer forRegistry(@NonNull PrometheusRegistry registry) {
        return byRegistry.computeIfAbsent(registry, PrometheusYojTracer::new);
    }

    @Override
    public Timer startTotalDurationTimer(String txName) {
        return totalDuration.labelValues(txName).startTimer()::close;
    }

    @Override
    public Timer startAttemptDurationTimer(String txName) {
        return attemptDuration.labelValues(txName).startTimer()::close;
    }

    @Override
    public void onCommit(String txName) {
        results.labelValues(txName, "commit").inc();
    }

    @Override
    public void onRollback(String txName) {
        results.labelValues(txName, "rollback").inc();
    }

    @Override
    public void onDryRun(String txName) {
        results.labelValues(txName, "dry_run").inc();
    }

    @Override
    public void onRetry(String txName, Throwable exception) {
        retries.labelValues(txName, Strings.removeSuffix(exception.getClass().getSimpleName(), "Exception")).inc();
    }

    @Override
    public void onFailure(String txName, Throwable exception) {
        results.labelValues(txName, "fail").inc();
    }

    @Override
    public void observeAttempts(String txName, int attemptCount) {
        attempts.labelValues(txName).observe(attemptCount);
    }

    @Override
    public SessionPool registerSessionPool(String repositoryLabel, Supplier<SessionPoolStats> statsSupplier) {
        return sessions.registerPool(repositoryLabel, statsSupplier);
    }

    /** Supplies the shared tracer for {@link PrometheusRegistry#defaultRegistry}. */
    public static final class DefaultProvider implements YojTracer.Provider {
        @Override
        public YojTracer getTracer() {
            return forRegistry(PrometheusRegistry.defaultRegistry);
        }
    }
}
