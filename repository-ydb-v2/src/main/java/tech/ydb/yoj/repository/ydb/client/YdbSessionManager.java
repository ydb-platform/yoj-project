package tech.ydb.yoj.repository.ydb.client;

import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import lombok.NonNull;
import tech.ydb.core.Result;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.metrics.SupplierCollector;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static tech.ydb.yoj.util.lang.Interrupts.isThreadInterrupted;

@InternalApi
public final class YdbSessionManager implements SessionManager {
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

    private final TableClient tableClient;
    private final Duration sessionTimeout;

    public YdbSessionManager(@NonNull TableClient tableClient, @NonNull String repositoryName, @NonNull Duration sessionCreationTimeout) {
        this.tableClient = tableClient;
        this.sessionTimeout = getSessionTimeout(sessionCreationTimeout);

        legacyCollector
                .labels("pending_acquire_count").supplier(() -> tableClient.sessionPoolStats().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> tableClient.sessionPoolStats().getAcquiredCount())
                .labels("idle_count").supplier(() -> tableClient.sessionPoolStats().getIdleCount());

        sessionPoolSettings
                .labels(repositoryName, "min_size").set(tableClient.sessionPoolStats().getMinSize());
        sessionPoolSettings
                .labels(repositoryName, "max_size").set(tableClient.sessionPoolStats().getMaxSize());

        sessionPoolCounters
                .labels(repositoryName, "requested_total").supplier(() -> tableClient.sessionPoolStats().getRequestedTotal())
                .labels(repositoryName, "acquired_total").supplier(() -> tableClient.sessionPoolStats().getAcquiredTotal())
                .labels(repositoryName, "released_total").supplier(() -> tableClient.sessionPoolStats().getReleasedTotal())
                .labels(repositoryName, "created_total").supplier(() -> tableClient.sessionPoolStats().getCreatedTotal())
                .labels(repositoryName, "deleted_total").supplier(() -> tableClient.sessionPoolStats().getDeletedTotal())
                .labels(repositoryName, "failed_total").supplier(() -> tableClient.sessionPoolStats().getFailedTotal());
    }

    @Override
    public Session getSession() {
        CompletableFuture<Result<Session>> future = tableClient.createSession(sessionTimeout);
        try {
            Result<Session> result = future.get();
            YdbValidator.validate("session create", result.getStatus().getCode(), result.toString());
            return result.getValue();
        } catch (CancellationException | CompletionException | ExecutionException | InterruptedException e) {
            // We need to cancel future bacause in other case we can get session leak
            future.cancel(false);

            if (isThreadInterrupted(e)) {
                Thread.currentThread().interrupt();
                throw new QueryInterruptedException("get session interrupted", e);
            }
            YdbValidator.checkGrpcContextStatus(e.getMessage(), e);

            throw new UnavailableException("DB is unavailable", e);
        }
    }

    private static Duration getSessionTimeout(Duration timeout) {
        Duration max = Duration.ofMinutes(5);
        if (Duration.ZERO.equals(timeout) || timeout.compareTo(max) > 0) {
            return max;
        }
        return timeout;
    }

    @Override
    //todo: client load balancing
    public void warmup() {
        Session session = null;
        int maxRetrySessionCreateCount = 10;
        for (int i = 0; i < maxRetrySessionCreateCount; i++) {
            try {
                session = getSession();
                break;
            } catch (RetryableException ex) {
                if (i == maxRetrySessionCreateCount - 1) {
                    throw ex;
                }
            }
        }
        if (session != null) {
            session.close();
        }
    }
}
