package tech.ydb.yoj.prometheus;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBucket;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.junit.Test;
import tech.ydb.yoj.repository.db.YojTracer.SessionPoolStats;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;
import static org.assertj.core.groups.Tuple.tuple;

public class PrometheusYojTracerTest {
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final PrometheusYojTracer tracer = PrometheusYojTracer.forRegistry(registry);

    @Test
    public void transactionMetricNamesLabelsAndValuesArePreserved() {
        tracer.startTotalDurationTimer("orders").close();
        tracer.startAttemptDurationTimer("orders").close();
        tracer.observeAttempts("orders", 3);
        tracer.onCommit("orders");
        tracer.onRollback("orders");
        tracer.onDryRun("orders");
        tracer.onFailure("orders");
        tracer.onRetry("orders", new OptimisticLockException("retry"));

        MetricSnapshots scrape = registry.scrape();
        var results = (CounterSnapshot) metric(scrape, "tx_result");
        assertThat(results.getDataPoints()).hasSize(4).allSatisfy(point -> {
            assertThat(point.getLabels().get("tx_name")).isEqualTo("orders");
            assertThat(point.getValue()).isEqualTo(1);
        });
        assertThat(results.getDataPoints()).extracting(point -> point.getLabels().get("result"))
                .containsExactlyInAnyOrder("commit", "rollback", "dry_run", "fail");
        var retries = (CounterSnapshot) metric(scrape, "tx_retries");
        assertThat(retries.getDataPoints()).singleElement().satisfies(point -> {
            assertThatObject(point.getLabels()).isEqualTo(Labels.of("tx_name", "orders", "reason", "OptimisticLock"));
            assertThat(point.getValue()).isEqualTo(1);
        });
        for (String name : new String[]{"tx_total_duration_seconds", "tx_attempt_duration_seconds"}) {
            var histogram = (HistogramSnapshot) metric(scrape, name);
            assertThat(histogram.getDataPoints()).singleElement().satisfies(point -> {
                assertThatObject(point.getLabels()).isEqualTo(Labels.of("tx_name", "orders"));
                assertThat(point.getCount()).isEqualTo(1);
                assertThat(point.getSum()).isNotNegative();
                assertThat(point.getNativeSchema()).isEqualTo(HistogramSnapshot.CLASSIC_HISTOGRAM);
                assertThat(point.getClassicBuckets()).extracting(ClassicHistogramBucket::getUpperBound)
                        .containsExactly(.001, .0025, .005, .0075, .01, .025, .05, .075, .1, .25, .5, .75,
                                1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, Double.POSITIVE_INFINITY);
            });
        }
        var attempts = (HistogramSnapshot) metric(scrape, "tx_attempts");
        assertThat(attempts.getDataPoints()).singleElement().satisfies(point -> {
            assertThatObject(point.getLabels()).isEqualTo(Labels.of("tx_name", "orders"));
            assertThat(point.getCount()).isEqualTo(1);
            assertThat(point.getSum()).isEqualTo(3);
        });
    }

    @Test
    public void customRegistriesAreIsolatedAndShareOneTracerEach() {
        assertThat(PrometheusYojTracer.forRegistry(registry)).isSameAs(tracer);
        var otherRegistry = new PrometheusRegistry();
        var otherTracer = PrometheusYojTracer.forRegistry(otherRegistry);
        tracer.onCommit("orders");
        otherTracer.onCommit("billing");

        assertThat(metric(registry.scrape(), "tx_result").getDataPoints())
                .singleElement().satisfies(point -> assertThat(point.getLabels().get("tx_name")).isEqualTo("orders"));
        assertThat(metric(otherRegistry.scrape(), "tx_result").getDataPoints())
                .singleElement().satisfies(point -> assertThat(point.getLabels().get("tx_name")).isEqualTo("billing"));
    }

    @Test
    public void poolCloseRemovesMetricsAndStopsReadingSupplier() {
        var reads = new AtomicInteger();
        var pool = tracer.registerSessionPool("orders", () -> {
            reads.incrementAndGet();
            return new SessionPoolStats(1, 10, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        });
        pool.startAcquireTimer().close();

        MetricSnapshots scrape = registry.scrape();
        var settings = (GaugeSnapshot) metric(scrape, "ydb_session_manager_pool_settings");
        assertThat(settings.getDataPoints()).hasSize(2).allSatisfy(point ->
                assertThat(point.getLabels().get("repository")).isEqualTo("orders"));
        assertThat(settings.getDataPoints()).extracting(point -> point.getLabels().get("type"),
                GaugeSnapshot.GaugeDataPointSnapshot::getValue)
                .containsExactlyInAnyOrder(tuple("min_size", 1.0),
                        tuple("max_size", 10.0));
        var counters = (CounterSnapshot) metric(scrape, "ydb_session_manager_pool_counters");
        assertThat(counters.getDataPoints()).hasSize(6).allSatisfy(point ->
                assertThat(point.getLabels().get("repository")).isEqualTo("orders"));
        assertThat(counters.getDataPoints()).extracting(point -> point.getLabels().get("type"),
                CounterSnapshot.CounterDataPointSnapshot::getValue).containsExactlyInAnyOrder(
                        tuple("acquired_total", 6.0),
                        tuple("released_total", 7.0),
                        tuple("requested_total", 8.0),
                        tuple("created_total", 9.0),
                        tuple("failed_total", 10.0),
                        tuple("deleted_total", 11.0));
        var legacy = (GaugeSnapshot) metric(scrape, "ydb_session_manager_pool_stats");
        assertThat(legacy.getDataPoints()).extracting(point -> point.getLabels().get("type"),
                GaugeSnapshot.GaugeDataPointSnapshot::getValue).containsExactlyInAnyOrder(
                        tuple("idle_count", 3.0),
                        tuple("acquired_count", 4.0),
                        tuple("pending_acquire_count", 5.0));
        var duration = (HistogramSnapshot) metric(scrape, "ydb_session_manager_pool_acquire_duration_seconds");
        assertThat(duration.getDataPoints()).singleElement().satisfies(point -> {
            assertThatObject(point.getLabels()).isEqualTo(Labels.of("repository", "orders"));
            assertThat(point.getCount()).isEqualTo(1);
        });

        int readsBeforeClose = reads.get();
        pool.close();
        pool.close();
        assertThat(registry.scrape().stream().flatMap(metric -> metric.getDataPoints().stream())).isEmpty();
        assertThat(reads).hasValue(readsBeforeClose);
    }

    @Test
    public void closingReplacedPoolPreservesReplacementAndOtherPools() {
        var stats = new SessionPoolStats(0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var oldPool = tracer.registerSessionPool("orders", () -> stats);
        var replacement = tracer.registerSessionPool("orders", () -> stats);
        var otherPool = tracer.registerSessionPool("billing", () -> stats);
        oldPool.close();
        var settings = metric(registry.scrape(), "ydb_session_manager_pool_settings");
        assertThat(settings.getDataPoints()).hasSize(4);
        replacement.close();
        settings = metric(registry.scrape(), "ydb_session_manager_pool_settings");
        assertThat(settings.getDataPoints()).hasSize(2).allSatisfy(point ->
                assertThat(point.getLabels().get("repository")).isEqualTo("billing"));
        otherPool.close();
        assertThat(registry.scrape().stream().flatMap(metric -> metric.getDataPoints().stream())).isEmpty();
    }

    @Test
    public void lateAcquisitionsCannotRecreateClosedPoolMetricsOrUpdateReplacement() {
        var stats = new SessionPoolStats(0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var oldPool = tracer.registerSessionPool("orders", () -> stats);
        var pendingTimer = oldPool.startAcquireTimer();
        oldPool.close();
        oldPool.startAcquireTimer().close();
        pendingTimer.close();
        assertThat(registry.scrape().stream().flatMap(metric -> metric.getDataPoints().stream())).isEmpty();

        var replacement = tracer.registerSessionPool("orders", () -> stats);
        oldPool.startAcquireTimer().close();
        assertThat(metric(registry.scrape(), "ydb_session_manager_pool_acquire_duration_seconds").getDataPoints()).isEmpty();
        replacement.startAcquireTimer().close();
        var duration = (HistogramSnapshot) metric(registry.scrape(), "ydb_session_manager_pool_acquire_duration_seconds");
        assertThat(duration.getDataPoints()).singleElement().satisfies(point -> assertThat(point.getCount()).isEqualTo(1));
        replacement.close();
    }

    private static MetricSnapshot metric(MetricSnapshots snapshots, String name) {
        return snapshots.stream().filter(metric -> metric.getMetadata().getName().equals(name)).findFirst().orElseThrow();
    }
}
