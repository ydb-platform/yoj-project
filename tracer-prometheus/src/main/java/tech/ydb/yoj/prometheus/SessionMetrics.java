package tech.ydb.yoj.prometheus;

import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.tracer.YojTracer;
import tech.ydb.yoj.tracer.YojTracer.SessionPoolStats;
import tech.ydb.yoj.tracer.YojTracer.Timer;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

final class SessionMetrics {
    private static final Logger log = LoggerFactory.getLogger(SessionMetrics.class);

    private final Map<String, Pool> pools = new ConcurrentHashMap<>();
    // Legacy gauges have no "repository" label, so they report the most recently initialized session pool
    private volatile String lastInitializedLabel;

    private final GaugeWithCallback legacyCollector = GaugeWithCallback.builder()
            .name("ydb_session_manager_pool_stats")
            .help("YDB SDK Session pool statistics (as gauges with instant values)")
            .labelNames("type")
            .callback(callback -> {
                SessionPoolStats stats = readStats(lastInitializedLabel);

                if (stats != null) {
                    callback.call(stats.pendingAcquireCount(), "pending_acquire_count");
                    callback.call(stats.acquiredCount(), "acquired_count");
                    callback.call(stats.idleCount(), "idle_count");
                }
            })
            .build();

    private final Gauge sessionPoolSettings = Gauge.builder()
            .name("ydb_session_manager_pool_settings")
            .help("YDB SDK Session pool settings")
            .labelNames("repository", "type")
            .build();
    private final CounterWithCallback sessionPoolCounters = CounterWithCallback.builder()
            .name("ydb_session_manager_pool_counters")
            .help("YDB SDK Session pool statistics (as total counters)")
            .labelNames("repository", "type")
            .callback(callback -> pools.keySet().forEach(label -> {
                SessionPoolStats stats = readStats(label);

                if (stats != null) {
                    callback.call(stats.requestedTotal(), label, "requested_total");
                    callback.call(stats.acquiredTotal(), label, "acquired_total");
                    callback.call(stats.releasedTotal(), label, "released_total");
                    callback.call(stats.createdTotal(), label, "created_total");
                    callback.call(stats.deletedTotal(), label, "deleted_total");
                    callback.call(stats.failedTotal(), label, "failed_total");
                }
            }))
            .build();

    private final Histogram acquireDurationSeconds = Histogram.builder()
            .name("ydb_session_manager_pool_acquire_duration_seconds")
            .help("Duration of 'acquire session from pool' (as a histogram)")
            .labelNames("repository")
            .classicOnly()
            .classicUpperBounds(PrometheusYojTracer.DURATION_BUCKETS)
            .build();

    SessionMetrics(PrometheusRegistry registry) {
        registry.register(legacyCollector);
        registry.register(sessionPoolSettings);
        registry.register(sessionPoolCounters);
        registry.register(acquireDurationSeconds);
    }

    synchronized YojTracer.SessionPool registerPool(@NonNull String label, @NonNull Supplier<SessionPoolStats> supplier) {
        SessionPoolStats stats = supplier.get();
        sessionPoolSettings.labelValues(label, "min_size").set(stats.minSize());
        sessionPoolSettings.labelValues(label, "max_size").set(stats.maxSize());
        var pool = new Pool(label, supplier);
        pools.put(label, pool);
        lastInitializedLabel = label;
        return pool;
    }

    private synchronized void closePool(Pool pool) {
        if (pools.remove(pool.label, pool)) {
            sessionPoolSettings.remove(pool.label, "min_size");
            sessionPoolSettings.remove(pool.label, "max_size");
            acquireDurationSeconds.remove(pool.label);
        }
    }

    @Nullable
    private synchronized SessionPoolStats readStats(@Nullable String label) {
        Pool pool = label == null ? null : pools.get(label);
        if (pool == null) {
            return null;
        }
        try {
            return pool.supplier.get();
        } catch (Exception e) {
            log.error("Could not read session pool stats for repository {}", label, e);
            return null;
        }
    }

    private final class Pool implements YojTracer.SessionPool {
        private final String label;
        private final Supplier<SessionPoolStats> supplier;

        private Pool(String label, Supplier<SessionPoolStats> supplier) {
            this.label = label;
            this.supplier = supplier;
        }

        @Override
        public Timer startAcquireTimer() {
            synchronized (SessionMetrics.this) {
                if (pools.get(label) != this) {
                    return Timer.NONE;
                }
                return acquireDurationSeconds.labelValues(label).startTimer()::close;
            }
        }

        @Override
        public void close() {
            closePool(this);
        }
    }
}
