package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.yandex.ydb.core.auth.AuthProvider;
import com.yandex.ydb.core.auth.NopAuthProvider;
import com.yandex.ydb.core.grpc.GrpcTransport;
import io.grpc.ClientInterceptor;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.SchemaOperations;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbPaths;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;
import tech.ydb.yoj.repository.ydb.client.YdbSessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;
import tech.ydb.yoj.repository.ydb.compatibility.YdbDataCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.compatibility.YdbSchemaCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.statement.Statement;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.canonicalDatabase;

public class YdbRepository implements Repository {
    private static final Logger log = LoggerFactory.getLogger(YdbRepository.class);

    @Getter
    private final YdbSchemaOperations schemaOperations;
    @Getter
    private final SessionManager sessionManager;
    private final GrpcTransport transport;
    @Getter
    private final String tablespace;

    @Getter
    private final YdbConfig config;

    private final ConcurrentMap<String, Class<? extends Entity<?>>> entityClassesByTableName;

    public YdbRepository(@NonNull YdbConfig config) {
        this(config, NopAuthProvider.INSTANCE);
    }

    public YdbRepository(@NonNull YdbConfig config, @NonNull AuthProvider authProvider) {
        this(config, authProvider, List.of());
    }

    public YdbRepository(@NonNull YdbConfig config, @NonNull AuthProvider authProvider, List<ClientInterceptor> interceptors) {
        this.config = config;
        this.tablespace = YdbPaths.canonicalTablespace(config.getTablespace());
        this.entityClassesByTableName = new ConcurrentHashMap<>();
        this.transport = makeGrpcTransport(config, authProvider, interceptors);
        this.sessionManager = new YdbSessionManager(config, transport);
        this.schemaOperations = buildSchemaOperations(config.getTablespace(), transport, sessionManager);
    }

    @NonNull
    protected YdbSchemaOperations buildSchemaOperations(@NonNull String tablespace, GrpcTransport transport, SessionManager sessionManager) {
        return new YdbSchemaOperations(tablespace, sessionManager, transport);
    }

    public final void checkDataCompatibility(List<Class<? extends Entity>> entities) {
        checkDataCompatibility(entities, YdbDataCompatibilityChecker.Config.DEFAULT);
    }

    public final void checkSchemaCompatibility(List<Class<? extends Entity>> entities) {
        checkSchemaCompatibility(entities, YdbSchemaCompatibilityChecker.Config.DEFAULT);
    }

    public final void checkSchemaCompatibility(
            List<Class<? extends Entity>> entities,
            YdbSchemaCompatibilityChecker.Config config
    ) {
        new YdbSchemaCompatibilityChecker(entities, this, config).run();
    }

    public final void checkDataCompatibility(
            List<Class<? extends Entity>> entities,
            YdbDataCompatibilityChecker.Config config
    ) {
        new YdbDataCompatibilityChecker(entities, this, config).run();
    }

    private GrpcTransport makeGrpcTransport(@NonNull YdbConfig config, AuthProvider authProvider, List<ClientInterceptor> interceptors) {
        GrpcTransport.Builder transportBuilder;
        if (!Strings.isNullOrEmpty(config.getDiscoveryEndpoint())) {
            transportBuilder = GrpcTransport.forEndpoint(config.getDiscoveryEndpoint(), canonicalDatabase(config.getDatabase()))
                    .withChannelInitializer(channelBuilder -> {
                        initializeTcpKeepAlive(channelBuilder);
                        initializeYdbMaxInboundMessageSize(channelBuilder);
                        initializeChannelInterceptors(channelBuilder, interceptors);
                    });
        } else {
            List<HostAndPort> endpoints = config.getEndpoints();
            if (endpoints == null || endpoints.isEmpty()) {
                throw new IllegalArgumentException("one of [discoveryEndpoint, endpoints] must be set");
            }
            transportBuilder = GrpcTransport.forHosts(endpoints)
                    .withDataBase(canonicalDatabase(config.getDatabase()))
                    .withChannelInitializer(channelBuilder -> {
                        initializeTcpKeepAlive(channelBuilder);
                        initializeYdbMaxInboundMessageSize(channelBuilder);
                        initializeChannelInterceptors(channelBuilder, interceptors);
                    });
        }

        if (config.isUseTLS()) {
            if (config.isUseTrustStore()) {
                transportBuilder.withSecureConnection();
            } else if (config.getRootCA() != null) {
                transportBuilder.withSecureConnection(config.getRootCA());
            } else {
                throw new IllegalArgumentException("you must either set useTrustStore=true or specify rootCA content");
            }
        }

        return transportBuilder
                .withAuthProvider(authProvider)
                .build();
    }

    private void initializeChannelInterceptors(NettyChannelBuilder channelBuilder, List<ClientInterceptor> interceptors) {
        channelBuilder.intercept(interceptors);
    }

    private void initializeTcpKeepAlive(NettyChannelBuilder channelBuilder) {
        channelBuilder
                .keepAliveTime(config.getTcpKeepaliveTime().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(config.getTcpKeepaliveTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(true);
    }

    private void initializeYdbMaxInboundMessageSize(NettyChannelBuilder channelBuilder) {
        // See com.yandex.ydb.core.grpc.GrpcTransport#createChannel
        // This fixes incompatibility with old gRPC without recompiling ydb-sdk-core
        // https://github.com/grpc/grpc-java/issues/8313
        channelBuilder.maxInboundMessageSize(64 << 20); // 64 MiB
    }

    @Override
    public boolean healthCheck() {
        return getSessionManager().healthCheck();
    }

    @Override
    public void shutdown() {
        getSessionManager().shutdown();
        transport.close();
    }

    @Override
    public void createTablespace() {
        schemaOperations.createTablespace();
    }

    @Override
    public Set<Class<? extends Entity<?>>> tables() {
        return schemaOperations.getTableNames().stream()
                .map(entityClassesByTableName::get)
                .filter(Objects::nonNull)
                .collect(toUnmodifiableSet());
    }

    @Override
    public RepositoryTransaction startTransaction(TxOptions options) {
        return new YdbRepositoryTransaction<>(this, options);
    }

    @Override
    public String makeSnapshot() {
        String snapshotPath = schemaOperations.getTablespace() + ".snapshot-" + UUID.randomUUID() + "/";
        schemaOperations.snapshot(snapshotPath);
        return snapshotPath;
    }

    @Override
    public void loadSnapshot(String id) {
        String current = schemaOperations.getTablespace();

        schemaOperations.getTableNames().forEach(schemaOperations::dropTable);
        schemaOperations.getDirectoryNames().stream()
                .filter(name -> !schemaOperations.isSnapshotDirectory(name))
                .forEach(schemaOperations::removeDirectoryRecursive);

        schemaOperations.setTablespace(id);
        schemaOperations.snapshot(current);
        schemaOperations.setTablespace(current);
        // NB: We use getSessionManager() method to allow mocking YdbRepository
        getSessionManager().invalidateAllSessions();
    }

    @Override
    public void dropDb() {
        try {
            schemaOperations.removeTablespace();
            entityClassesByTableName.clear();
        } catch (Exception e) {
            log.error("Could not drop all tables from tablespace", e);
        }
    }

    @Override
    public <T extends Entity<T>> SchemaOperations<T> schema(Class<T> c) {
        EntitySchema<T> schema = EntitySchema.of(c);
        return new SchemaOperations<>() {
            @Override
            public void create() {
                String tableName = schema.getName();
                schemaOperations.createTable(tableName, schema.flattenFields(), schema.flattenId(),
                        extractHint(), schema.getGlobalIndexes(), schema.getTtlModifier(), schema.getChangefeeds());
                if (!schema.isDynamic()) {
                    entityClassesByTableName.put(tableName, c);
                }
            }

            private YdbTableHint extractHint() {
                try {
                    Field ydbTableHintField = c.getDeclaredField("ydbTableHint");
                    ydbTableHintField.setAccessible(true);
                    return (YdbTableHint) ydbTableHintField.get(null);
                } catch (NoSuchFieldException | IllegalAccessException ignored) {
                    return null;
                }
            }

            @Override
            public void drop() {
                String tableName = schema.getName();
                schemaOperations.dropTable(tableName);
                entityClassesByTableName.remove(tableName);
            }

            @Override
            public boolean exists() {
                String tableName = schema.getName();
                boolean exists = schemaOperations.hasTable(tableName);
                if (!schema.isDynamic()) {
                    if (exists) {
                        entityClassesByTableName.put(tableName, c);
                    } else {
                        entityClassesByTableName.remove(tableName);
                    }
                }
                return exists;
            }
        };
    }

    @Value
    public static class Query<PARAMS> {
        Statement<PARAMS, ?> statement;
        List<PARAMS> values = new ArrayList<>();

        public Query(Statement<PARAMS, ?> statement, PARAMS value) {
            this.statement = statement;
            values.add(value);
        }

        @SuppressWarnings("unchecked")
        public Query<PARAMS> merge(Query<?> ps) {
            values.addAll((Collection<? extends PARAMS>) ps.getValues());
            return this;
        }
    }
}
