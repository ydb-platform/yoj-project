package tech.ydb.yoj.repository.ydb;

import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import tech.ydb.table.TableClient;
import tech.ydb.yoj.repository.ydb.metrics.SupplierCollector;

/*package*/ final class SessionMetrics implements AutoCloseable {
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

    private final String label;

    /*package*/ SessionMetrics(TableClient tableClient, YdbRepository.Settings.Metrics metrics) {
        this.label = metrics.repositoryLabel();

        legacyCollector
                .labels("pending_acquire_count").supplier(() -> tableClient.sessionPoolStats().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> tableClient.sessionPoolStats().getAcquiredCount())
                .labels("idle_count").supplier(() -> tableClient.sessionPoolStats().getIdleCount());

        sessionPoolSettings.labels(label, "min_size").set(tableClient.sessionPoolStats().getMinSize());
        sessionPoolSettings.labels(label, "max_size").set(tableClient.sessionPoolStats().getMaxSize());

        sessionPoolCounters
                .labels(label, "requested_total").supplier(() -> tableClient.sessionPoolStats().getRequestedTotal())
                .labels(label, "acquired_total").supplier(() -> tableClient.sessionPoolStats().getAcquiredTotal())
                .labels(label, "released_total").supplier(() -> tableClient.sessionPoolStats().getReleasedTotal())
                .labels(label, "created_total").supplier(() -> tableClient.sessionPoolStats().getCreatedTotal())
                .labels(label, "deleted_total").supplier(() -> tableClient.sessionPoolStats().getDeletedTotal())
                .labels(label, "failed_total").supplier(() -> tableClient.sessionPoolStats().getFailedTotal());
    }

    @Override
    public void close() {
        sessionPoolSettings.remove(label, "min_size");
        sessionPoolSettings.remove(label, "max_size");

        sessionPoolCounters.remove(label, "requested_total");
        sessionPoolCounters.remove(label, "acquired_total");
        sessionPoolCounters.remove(label, "released_total");
        sessionPoolCounters.remove(label, "created_total");
        sessionPoolCounters.remove(label, "deleted_total");
        sessionPoolCounters.remove(label, "failed_total");
    }
}
