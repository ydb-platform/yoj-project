package tech.ydb.yoj.repository.ydb;

import lombok.Getter;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.table.TableClient;
import tech.ydb.topic.TopicClient;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;
import tech.ydb.yoj.repository.ydb.client.YdbSessionManager;
import tech.ydb.yoj.util.lang.Exceptions;

/*package*/ final class SessionClient implements AutoCloseable {
    private final TableClient tableClient;
    private final SchemeClient schemeClient;
    private final TopicClient topicClient;

    @Getter
    private final SessionManager sessionManager;

    @Getter
    private final YdbSchemaOperations schemaOperations;

    private final SessionMetrics metrics;

    /*package*/ SessionClient(YdbConfig config, YdbRepository.Settings repositorySettings, GrpcTransport transport) {
        this.tableClient = createClient(config, repositorySettings, transport);
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.topicClient = TopicClient.newClient(transport).build();

        this.sessionManager = new YdbSessionManager(tableClient, config.getSessionCreationTimeout());
        this.schemaOperations = new YdbSchemaOperations(
                config.getTablespace(), sessionManager, schemeClient, topicClient
        );

        this.metrics = new SessionMetrics(tableClient, repositorySettings.metrics());
    }

    public boolean isHealthy() {
        // We consider the database healthy if the number of sessions in the pool is greater than 0.
        // Bad sessions will be dropped either due to keep-alive or on the very first error that occurs in that session.
        //
        // If idleCount == 0, this may mean that the application has just started, or that the database cannot handle the load.
        // To account for that case, we check pendingAcquireCount (how many clients are waiting to acquire a session),
        // and if it’s more than maxSize of the client queue, we consider the database to be unhealthy.
        SessionPoolStats sessionPoolStats = tableClient.sessionPoolStats();
        return sessionPoolStats.getIdleCount() > 0 ||
                //todo: maybe we should consider pendingAcquireCount > 0 problematic, because there are clients waiting?
                sessionPoolStats.getPendingAcquireCount() <= sessionPoolStats.getMaxSize();
    }

    @Override
    public void close() {
        Exceptions.closeAll(metrics, tableClient, schemeClient, topicClient);
    }

    private static TableClient createClient(
            YdbConfig config, YdbRepository.Settings repositorySettings, GrpcTransport transport
    ) {
        return buildTableClient(repositorySettings, transport)
                .keepQueryText(false)
                .sessionKeepAliveTime(config.getSessionKeepAliveTime())
                .sessionMaxIdleTime(config.getSessionMaxIdleTime())
                .sessionPoolSize(config.getSessionPoolMin(), config.getSessionPoolMax())
                .build();
    }

    private static TableClient.Builder buildTableClient(
            YdbRepository.Settings repositorySettings, GrpcTransport transport
    ) {
        // TODO(nvamelichev@): Replace this with expression switch with type pattern as soon as we migrate to Java 21+
        var queryImplementation = repositorySettings.queryImplementation();
        if (queryImplementation instanceof QueryImplementation.TableService) {
            return TableClient.newClient(transport);
        } else if (queryImplementation instanceof QueryImplementation.QueryService) {
            return tech.ydb.query.impl.TableClientImpl.newClient(transport);
        } else {
            throw new UnsupportedOperationException("Unknown QueryImplementation: <" + queryImplementation.getClass() + ">");
        }
    }
}
