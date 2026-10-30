package tech.ydb.yoj.repository.ydb;

import io.prometheus.metrics.core.datapoints.Timer;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.table.SessionPoolStats;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SessionMetrics {
    private static final Logger log = LoggerFactory.getLogger(SessionMetrics.class);

    private static final Map<String, Supplier<SessionPoolStats>> statsSuppliersByPool = new ConcurrentHashMap<>();
    // Legacy gauges have no "repository" label, so they report the most recently initialized session pool
    private static volatile String lastInitializedLabel;

    private static final GaugeWithCallback legacyCollector = GaugeWithCallback.builder()
            .name("ydb_session_manager_pool_stats")
            .help("YDB SDK Session pool statistics (as gauges with instant values)")
            .labelNames("type")
            .callback(callback -> {
                SessionPoolStats stats = readStats(lastInitializedLabel);

                if (stats != null) {
                    callback.call(stats.getPendingAcquireCount(), "pending_acquire_count");
                    callback.call(stats.getAcquiredCount(), "acquired_count");
                    callback.call(stats.getIdleCount(), "idle_count");
                }
            })
            .register();

    private static final Gauge sessionPoolSettings = Gauge.builder()
            .name("ydb_session_manager_pool_settings")
            .help("YDB SDK Session pool settings")
            .labelNames("repository", "type")
            .register();
    private static final CounterWithCallback sessionPoolCounters = CounterWithCallback.builder()
            .name("ydb_session_manager_pool_counters")
            .help("YDB SDK Session pool statistics (as total counters)")
            .labelNames("repository", "type")
            .callback(callback -> statsSuppliersByPool.keySet().forEach(label -> {
                SessionPoolStats stats = readStats(label);

                if (stats != null) {
                    callback.call(stats.getRequestedTotal(), label, "requested_total");
                    callback.call(stats.getAcquiredTotal(), label, "acquired_total");
                    callback.call(stats.getReleasedTotal(), label, "released_total");
                    callback.call(stats.getCreatedTotal(), label, "created_total");
                    callback.call(stats.getDeletedTotal(), label, "deleted_total");
                    callback.call(stats.getFailedTotal(), label, "failed_total");
                }
            }))
            .register();

    // TODO(nvamelichev): Move common metrics logic into yoj-util
    private static final double[] DURATION_BUCKETS = {
            .001, .0025, .005, .0075,
            .01, .025, .05, .075,
            .1, .25, .5, .75,
            1, 2.5, 5, 7.5,
            10, 25, 50, 75,
            100
    };
    private static final Histogram acquireDurationSeconds = Histogram.builder()
            .name("ydb_session_manager_pool_acquire_duration_seconds")
            .help("Duration of 'acquire session from pool' (as a histogram)")
            .labelNames("repository")
            .classicOnly()
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    public static void init(String label, Supplier<SessionPoolStats> statsSupplier) {
        statsSuppliersByPool.put(label, statsSupplier);
        lastInitializedLabel = label;

        sessionPoolSettings.labelValues(label, "min_size").set(statsSupplier.get().getMinSize());
        sessionPoolSettings.labelValues(label, "max_size").set(statsSupplier.get().getMaxSize());
    }

    public static Timer acquireDurationSeconds(String label) {
        return acquireDurationSeconds.labelValues(label).startTimer();
    }

    @Nullable
    private static SessionPoolStats readStats(@Nullable String label) {
        try {
            return statsSuppliersByPool.get(label).get();
        } catch (Exception e) {
            log.error("Could not read session pool stats for repository {}", label, e);
            return null;
        }
    }
}
