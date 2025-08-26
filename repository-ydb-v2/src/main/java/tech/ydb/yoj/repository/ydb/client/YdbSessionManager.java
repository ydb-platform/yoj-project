package tech.ydb.yoj.repository.ydb.client;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.table.TableClient;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.YdbConfig;
import tech.ydb.yoj.repository.ydb.metrics.GaugeSupplierCollector;
import tech.ydb.yoj.util.function.MoreSuppliers;
import tech.ydb.yoj.util.function.MoreSuppliers.CloseableMemoizer;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static tech.ydb.yoj.util.lang.Interrupts.isThreadInterrupted;

@InternalApi
public final class YdbSessionManager implements SessionManager {
    private static final String PROP_CLIENT_IMPL = "tech.ydb.yoj.repository.ydb.client.impl";
    private static final String CLIENT_IMPL_TABLE = "table";
    private static final String CLIENT_IMPL_QUERY = "query";

    private static final GaugeSupplierCollector sessionStatCollector = GaugeSupplierCollector.build()
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_stats")
            .help("Session pool statistics")
            .labelNames("type")
            .register();

    private final YdbConfig config;
    private final CloseableMemoizer<TableClient> tableClient;
    private final SessionPoolStats emptyPoolStats;

    public YdbSessionManager(@NonNull YdbConfig config, GrpcTransport transport) {
        this.config = config;
        this.tableClient = MoreSuppliers.memoizeCloseable(() -> createClient(transport));
        this.emptyPoolStats = new EmptyPoolStats(config);

        sessionStatCollector
                .labels("pending_acquire_count").supplier(() -> getPoolStats().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> getPoolStats().getAcquiredCount())
                .labels("idle_count").supplier(() -> getPoolStats().getIdleCount());
    }

    private TableClient getTableClient() {
        return tableClient.get();
    }

    private SessionPoolStats getPoolStats() {
        TableClient activeClient = tableClient.orElseNull();
        return activeClient == null ? emptyPoolStats : activeClient.sessionPoolStats();
    }

    private TableClient createClient(GrpcTransport transport) {
        var impl = System.getProperty(PROP_CLIENT_IMPL, CLIENT_IMPL_TABLE);
        var bldr = switch (impl) {
            case CLIENT_IMPL_TABLE -> tech.ydb.table.TableClient.newClient(transport);
            case CLIENT_IMPL_QUERY -> tech.ydb.query.impl.TableClientImpl.newClient(transport);
            default -> throw new IllegalArgumentException("Unknown TableClient.Builder implementation: '" + impl + "'");
        };
        return bldr
                .keepQueryText(false)
                .sessionKeepAliveTime(config.getSessionKeepAliveTime())
                .sessionMaxIdleTime(config.getSessionMaxIdleTime())
                .sessionPoolSize(config.getSessionPoolMin(), config.getSessionPoolMax())
                .build();
    }

    @Override
    public Session getSession() {
        CompletableFuture<Result<Session>> future = getTableClient().createSession(getSessionTimeout());
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

    private Duration getSessionTimeout() {
        Duration max = Duration.ofMinutes(5);
        Duration configTimeout = config.getSessionCreationTimeout();
        return Duration.ZERO.equals(configTimeout) || configTimeout.compareTo(max) > 0 ? max : configTimeout;
    }

    @Override
    public void release(Session session) {
        session.close();
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
            release(session);
        }
    }

    @Override
    @VisibleForTesting
    public synchronized void invalidateAllSessions() {
        shutdown();
        tableClient.reset();
    }

    @Override
    public void shutdown() {
        tableClient.close();
    }

    @Override
    public boolean healthCheck() {
        // We consider the database healthy if the number of sessions in the pool is greater than 0.
        // Bad sessions will be dropped either due to keep-alive or on the very first error that occurs in that session.
        //
        // If idleCount == 0, this may mean that the application has just started, or that the database cannot handle the load.
        // To account for that case, we check pendingAcquireCount (how many clients are waiting to acquire a session),
        // and if it’s more than maxSize of the client queue, we consider the database to be unhealthy.
        SessionPoolStats sessionPoolStats = getPoolStats();
        return sessionPoolStats.getIdleCount() > 0 ||
                //todo: maybe we should consider pendingAcquireCount > 0 problematic, because there are clients waiting?
                sessionPoolStats.getPendingAcquireCount() <= sessionPoolStats.getMaxSize();
    }

    @Getter
    private static class EmptyPoolStats implements SessionPoolStats {
        private final int minSize;
        private final int maxSize;

        public EmptyPoolStats(@NonNull YdbConfig config) {
            this.minSize = config.getSessionPoolMin();
            this.maxSize = config.getSessionPoolMax();
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
    }
}
