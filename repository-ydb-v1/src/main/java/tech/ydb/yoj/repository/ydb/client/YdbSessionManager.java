package tech.ydb.yoj.repository.ydb.client;

import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import com.yandex.ydb.table.stats.SessionPoolStats;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.exception.QueryInterruptedException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.ydb.YdbConfig;
import tech.ydb.yoj.repository.ydb.metrics.GaugeSupplierCollector;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static tech.ydb.yoj.repository.ydb.client.YdbValidator.checkGrpcContextStatus;
import static tech.ydb.yoj.repository.ydb.client.YdbValidator.validate;
import static tech.ydb.yoj.util.lang.Interrupts.isThreadInterrupted;

public class YdbSessionManager implements SessionManager {
    private static final GaugeSupplierCollector sessionStatCollector = GaugeSupplierCollector.build()
            .namespace("ydb")
            .subsystem("session_manager")
            .name("pool_stats")
            .help("Session pool statistics")
            .labelNames("type")
            .register();

    private final YdbConfig config;
    private final GrpcTableRpc tableRpc;
    @Getter
    private TableClient tableClient;

    public YdbSessionManager(@NonNull YdbConfig config, GrpcTransport transport) {
        this.config = config;
        this.tableRpc = GrpcTableRpc.useTransport(transport);
        this.tableClient = createClient();

        sessionStatCollector
                .labels("pending_acquire_count").supplier(() -> tableClient.getSessionPoolStats().getPendingAcquireCount())
                .labels("acquired_count").supplier(() -> tableClient.getSessionPoolStats().getAcquiredCount())
                .labels("idle_count").supplier(() -> tableClient.getSessionPoolStats().getIdleCount())
                .labels("disconnected_count").supplier(() -> tableClient.getSessionPoolStats().getDisconnectedCount());
    }

    private TableClient createClient() {
        return TableClient.newClient(tableRpc)
                .keepQueryText(false)
                .queryCacheSize(0)
                .sessionCreationMaxRetries(config.getSessionCreationMaxRetries())
                .sessionKeepAliveTime(config.getSessionKeepAliveTime())
                .sessionMaxIdleTime(config.getSessionMaxIdleTime())
                .sessionPoolSize(config.getSessionPoolMin(), config.getSessionPoolMax())
                .build();
    }

    @Override
    public Session getSession() {
        CompletableFuture<Result<Session>> future = tableClient.getOrCreateSession(getSessionTimeout());
        try {
            Result<Session> result = future.get();
            validate("session create", result.getCode(), result.toString());
            return result.expect("Can't get session");
        } catch (CancellationException | CompletionException | ExecutionException | InterruptedException e) {
            // We need to cancel future bacause in other case we can get session leak
            future.cancel(false);

            if (isThreadInterrupted(e)) {
                Thread.currentThread().interrupt();
                throw new QueryInterruptedException("get session interrupted", e);
            }
            checkGrpcContextStatus(e.getMessage(), e);

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
        session.release();
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
    public synchronized void invalidateAllSessions() {
        shutdown();
        tableClient = createClient();
    }

    @Override
    public void shutdown() {
        tableClient.close();
    }

    @Override
    public boolean healthCheck() {
        // Базу считаем живой, если кол-во сессий в пуле больше 0.
        // Плохие сессии отвалятся по кипэлайву или при первой же ошибке вознишей в этой сессии.
        // Если idle==0, это может означать, что приложение только-только запустилось,
        // или что база не справляется с нагрузкой - тогда смотрим сколько
        // в очереди на получении сессии и если больше чем maxSize самой очереди, то кажется не все ок с базой.
        SessionPoolStats sessionPoolStats = tableClient.getSessionPoolStats();
        return sessionPoolStats.getIdleCount() > 0 ||
                //todo: кажется стоит считать getPendingAcquireCount > 0 проблемой, тк это уже перегруз
                sessionPoolStats.getPendingAcquireCount() <= sessionPoolStats.getMaxSize();
    }
}
