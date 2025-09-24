package tech.ydb.yoj.repository.ydb;

import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.yoj.repository.ydb.metrics.SupplierCollector;

import java.util.function.Supplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SessionMetrics {
    private static final SupplierCollector legacyCollector = SupplierCollector.build()
            .type(Collector.Type.GAUGE)
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_stats")
            .help("YDB SDK Session pool statistics (as gauges with instant values)")
            .labelNames("type")
            .register();

    private static final Gauge sessionPoolSettings = Gauge.build()
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_settings")
            .help("YDB SDK Session pool settings")
            .labelNames("repository", "type")
            .register();
    private static final SupplierCollector sessionPoolCounters = SupplierCollector.build()
            .type(Collector.Type.COUNTER)
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_counters")
            .help("YDB SDK Session pool statistics (as total counters)")
            .labelNames("repository", "type")
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
    private static final Histogram acquireDurationSeconds = Histogram.build()
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_acquire_duration_seconds")
            .help("Duration of 'acquire session from pool' (as a histogram)")
            .labelNames("repository")
            .buckets(DURATION_BUCKETS)
            .register();
    
    public static void init(String label, Supplier<SessionPoolStats> statsSupplier) {
        legacyCollector
                .labels("pending_acquire_count").supplier(() -> statsSupplier.get().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> statsSupplier.get().getAcquiredCount())
                .labels("idle_count").supplier(() -> statsSupplier.get().getIdleCount());

        sessionPoolSettings.labels(label, "min_size").set(statsSupplier.get().getMinSize());
        sessionPoolSettings.labels(label, "max_size").set(statsSupplier.get().getMaxSize());

        sessionPoolCounters
                .labels(label, "requested_total").supplier(() -> statsSupplier.get().getRequestedTotal())
                .labels(label, "acquired_total").supplier(() -> statsSupplier.get().getAcquiredTotal())
                .labels(label, "released_total").supplier(() -> statsSupplier.get().getReleasedTotal())
                .labels(label, "created_total").supplier(() -> statsSupplier.get().getCreatedTotal())
                .labels(label, "deleted_total").supplier(() -> statsSupplier.get().getDeletedTotal())
                .labels(label, "failed_total").supplier(() -> statsSupplier.get().getFailedTotal());
    }

    public static Histogram.Timer acquireDurationSeconds(String label) {
        return acquireDurationSeconds.labels(label).startTimer();
    }
}
