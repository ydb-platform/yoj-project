package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.table.TableClient;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.ydb.YdbRepository.Settings;
import tech.ydb.yoj.repository.ydb.metrics.SupplierCollector;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@InternalApi
public final class TableClientWithMetrics implements TableClient {
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

    private final TableClient delegate;
    private final SessionPoolStats emptyPoolStats;
    private final String label;

    private final AtomicBoolean closed;

    private TableClientWithMetrics(TableClient delegate, SessionPoolStats emptyPoolStats, Settings.Metrics metrics) {
        this.closed = new AtomicBoolean();

        this.delegate = delegate;
        this.emptyPoolStats = emptyPoolStats;
        this.label = metrics.repositoryLabel();

        legacyCollector
                .labels("pending_acquire_count").supplier(() -> sessionPoolStats().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> sessionPoolStats().getAcquiredCount())
                .labels("idle_count").supplier(() -> sessionPoolStats().getIdleCount());

        sessionPoolSettings.labels(label, "min_size").set(sessionPoolStats().getMinSize());
        sessionPoolSettings.labels(label, "max_size").set(sessionPoolStats().getMaxSize());

        sessionPoolCounters
                .labels(label, "requested_total").supplier(() -> sessionPoolStats().getRequestedTotal())
                .labels(label, "acquired_total").supplier(() -> sessionPoolStats().getAcquiredTotal())
                .labels(label, "released_total").supplier(() -> sessionPoolStats().getReleasedTotal())
                .labels(label, "created_total").supplier(() -> sessionPoolStats().getCreatedTotal())
                .labels(label, "deleted_total").supplier(() -> sessionPoolStats().getDeletedTotal())
                .labels(label, "failed_total").supplier(() -> sessionPoolStats().getFailedTotal());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.delegate.close();
        }
    }

    @Override
    public CompletableFuture<Result<Session>> createSession(Duration duration) {
        Preconditions.checkState(!closed.get(), "Cannot call createSession() on a closed TableClient");

        var timer = acquireDurationSeconds.labels(label).startTimer();
        try {
            return delegate.createSession(duration)
                    .whenComplete((_1, _2) -> observeAcquireDuration(timer));
        } catch (Exception e) {
            // Could not create CompletableFuture for creating the session, or add a whenComplete() handler to the CF
            try {
                observeAcquireDuration(timer);
            } catch (Exception suppressed) {
                e.addSuppressed(e);
            }
            throw e;
        }
    }

    private void observeAcquireDuration(Histogram.Timer timer) {
        if (!closed.get()) {
            timer.observeDuration();
        }
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        Preconditions.checkState(!closed.get(), "Cannot call getScheduler() on a closed TableClient");
        return delegate.getScheduler();
    }

    @Override
    public SessionPoolStats sessionPoolStats() {
        return closed.get() ? emptyPoolStats : delegate.sessionPoolStats();
    }

    public static TableClient newClient(YdbConfig config, Settings repositorySettings, GrpcTransport transport) {
        var delegate = buildTableClient(repositorySettings, transport).keepQueryText(false)
                .sessionKeepAliveTime(config.getSessionKeepAliveTime())
                .sessionMaxIdleTime(config.getSessionMaxIdleTime())
                .sessionPoolSize(config.getSessionPoolMin(), config.getSessionPoolMax())
                .build();
        var emptyPoolStats = new EmptyPoolStats(config.getSessionPoolMin(), config.getSessionPoolMax());
        var metrics = repositorySettings.metrics();

        return new TableClientWithMetrics(delegate, emptyPoolStats, metrics);
    }

    private static TableClient.Builder buildTableClient(Settings repositorySettings, GrpcTransport transport) {
        // TODO(nvamelichev@): Replace this with expression switch with type pattern as soon as we migrate to Java 21+
        var queryImplementation = repositorySettings.queryImplementation();
        if (queryImplementation instanceof QueryImplementation.TableService) {
            return TableClient.newClient(transport);
        } else if (queryImplementation instanceof QueryImplementation.QueryService) {
            return tech.ydb.query.impl.TableClientImpl.newClient(transport);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown QueryImplementation: <" + queryImplementation.getClass() + ">"
            );
        }
    }

    private static final class EmptyPoolStats implements SessionPoolStats {
        private final int minSize;
        private final int maxSize;

        private EmptyPoolStats(int minSize, int maxSize) {
            this.minSize = minSize;
            this.maxSize = maxSize;
        }

        @Override
        public int getIdleCount() {
            return 0;
        }

        @Override
        public int getAcquiredCount() {
            return 0;
        }

        @Override
        public int getPendingAcquireCount() {
            return 0;
        }

        @Override
        public long getAcquiredTotal() {
            return 0;
        }

        @Override
        public long getReleasedTotal() {
            return 0;
        }

        @Override
        public long getRequestedTotal() {
            return 0;
        }

        @Override
        public long getCreatedTotal() {
            return 0;
        }

        @Override
        public long getFailedTotal() {
            return 0;
        }

        @Override
        public long getDeletedTotal() {
            return 0;
        }

        @Override
        public String toString() {
            return "EmptySessionPoolStats{minSize=" + minSize + ", maxSize=" + maxSize + "}";
        }

        public int getMinSize() {
            return this.minSize;
        }

        public int getMaxSize() {
            return this.maxSize;
        }
    }
}
