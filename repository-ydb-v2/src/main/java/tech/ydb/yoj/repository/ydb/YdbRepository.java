package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Strings;
import io.grpc.ClientInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.impl.SingleChannelTransport;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.SchemaOperations;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbPaths;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;
import tech.ydb.yoj.repository.ydb.client.YdbSessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbTableHint;
import tech.ydb.yoj.repository.ydb.compatibility.YdbDataCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.compatibility.YdbSchemaCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.util.function.MoreSuppliers;
import tech.ydb.yoj.util.function.MoreSuppliers.CloseableMemoizer;

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
import java.util.function.Supplier;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.canonicalDatabase;

public class YdbRepository implements Repository {
    private static final Logger log = LoggerFactory.getLogger(YdbRepository.class);

    private final GrpcTransport transport;
    private final CloseableMemoizer<SessionManager> sessionManager;
    private final Supplier<YdbSchemaOperations> schemaOperations;

    @Getter
    private final String tablespace;

    @Getter
    private final YdbConfig config;

    private final ConcurrentMap<String, TableDescriptor<?>> entityClassesByTableName;

    public YdbRepository(@NonNull YdbConfig config) {
        this(config, NopAuthProvider.INSTANCE);
    }

    public YdbRepository(@NonNull YdbConfig config, @NonNull AuthProvider authProvider) {
        this(config, authProvider, List.of());
    }

    public YdbRepository(@NonNull YdbConfig config, @NonNull AuthProvider authProvider, List<ClientInterceptor> interceptors) {
        this(config, makeGrpcTransport(config, authProvider, interceptors));
    }

    public YdbRepository(@NonNull YdbConfig config, @NonNull GrpcTransport transport) {
        this.config = config;
        this.tablespace = YdbPaths.canonicalTablespace(config.getTablespace());
        this.entityClassesByTableName = new ConcurrentHashMap<>();
        this.transport = transport;

        CloseableMemoizer<SessionManager> sessionManager = MoreSuppliers.memoizeCloseable(() -> new YdbSessionManager(config, transport));
        this.sessionManager = sessionManager;

        this.schemaOperations = MoreSuppliers.memoize(() -> buildSchemaOperations(config.getTablespace(), transport, sessionManager.get()));
    }

    private static GrpcTransport makeGrpcTransport(
            @NonNull YdbConfig config,
            @NonNull AuthProvider authProvider,
            List<ClientInterceptor> interceptors
    ) {
        boolean singleChannel = config.isUseSingleChannelTransport();
        return new LazyGrpcTransport(
                makeGrpcTransportBuilder(config, authProvider, interceptors),
                singleChannel ? SingleChannelTransport::new : GrpcTransportBuilder::build
        );
    }

    private static GrpcTransportBuilder makeGrpcTransportBuilder(@NonNull YdbConfig config, AuthProvider authProvider, List<ClientInterceptor> interceptors) {
        GrpcTransportBuilder transportBuilder;
        if (!Strings.isNullOrEmpty(config.getDiscoveryEndpoint())) {
            transportBuilder = GrpcTransport.forEndpoint(config.getDiscoveryEndpoint(), canonicalDatabase(config.getDatabase()))
                    .withChannelInitializer(channelBuilder -> {
                        initializeTcpKeepAlive(config, channelBuilder);
                        initializeYdbMaxInboundMessageSize(channelBuilder);
                        initializeChannelInterceptors(channelBuilder, interceptors);
                    });
        } else if (config.getHostAndPort() != null) {
            transportBuilder = GrpcTransport.forHost(config.getHostAndPort(), config.getDatabase())
                    .withChannelInitializer(channelBuilder -> {
                        initializeTcpKeepAlive(config, channelBuilder);
                        initializeYdbMaxInboundMessageSize(channelBuilder);
                        initializeChannelInterceptors(channelBuilder, interceptors);
                    });
        } else {
            throw new IllegalArgumentException("one of [discoveryEndpoint, hostAndPort] must be set");
        }

        if (config.getBalancingConfig() != null) {
            transportBuilder.withBalancingSettings(
                    switch (config.getBalancingConfig().getPolicy()) {
                        case USE_ALL_NODES -> BalancingSettings.fromPolicy(BalancingSettings.Policy.USE_ALL_NODES);
                        case USE_PREFERABLE_LOCATION -> BalancingSettings.fromLocation(config.getBalancingConfig().getPreferableLocation());
                        case DETECT_LOCAL_DC -> BalancingSettings.detectLocalDs();
                    }
            );
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

        return transportBuilder.withAuthProvider(authProvider);
    }

    private static void initializeChannelInterceptors(NettyChannelBuilder channelBuilder, List<ClientInterceptor> interceptors) {
        channelBuilder.intercept(interceptors);
    }

    private static void initializeTcpKeepAlive(YdbConfig config, NettyChannelBuilder channelBuilder) {
        channelBuilder
                .keepAliveTime(config.getTcpKeepaliveTime().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(config.getTcpKeepaliveTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(true);
    }

    private static void initializeYdbMaxInboundMessageSize(NettyChannelBuilder channelBuilder) {
        // See tech.ydb.core.grpc.GrpcTransport#createChannel
        // This fixes incompatibility with old gRPC without recompiling ydb-sdk-core
        // https://github.com/grpc/grpc-java/issues/8313
        channelBuilder.maxInboundMessageSize(64 << 20); // 64 MiB
    }

    public SessionManager getSessionManager() {
        return sessionManager.get();
    }

    public YdbSchemaOperations getSchemaOperations() {
        return schemaOperations.get();
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

    @Override
    public boolean healthCheck() {
        return getSessionManager().healthCheck();
    }

    @Override
    public void shutdown() {
        sessionManager.close();
        transport.close();
    }

    @Override
    public void createTablespace() {
        getSchemaOperations().createTablespace();
    }

    @Override
    public Set<TableDescriptor<?>> tables() {
        return getSchemaOperations().getTableNames().stream()
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
        YdbSchemaOperations schemaOperations = getSchemaOperations();

        String snapshotPath = schemaOperations.getTablespace() + ".snapshot-" + UUID.randomUUID() + "/";
        schemaOperations.snapshot(snapshotPath);
        return snapshotPath;
    }

    @Override
    public void loadSnapshot(String id) {
        YdbSchemaOperations schemaOperations = getSchemaOperations();

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
            getSchemaOperations().removeTablespace();
            entityClassesByTableName.clear();
        } catch (Exception e) {
            log.error("Could not drop all tables from tablespace", e);
        }
    }

    @Override
    public <T extends Entity<T>> SchemaOperations<T> schema(TableDescriptor<T> tableDescriptor) {
        EntitySchema<T> schema = EntitySchema.of(tableDescriptor.entityType());
        return new SchemaOperations<>() {
            @Override
            public void create() {
                String tableName = tableDescriptor.tableName();
                getSchemaOperations().createTable(
                        tableName,
                        schema.flattenFields(),
                        schema.flattenId(),
                        extractHint(),
                        schema.getGlobalIndexes(),
                        schema.getTtlModifier(),
                        schema.getChangefeeds()
                );
                entityClassesByTableName.put(tableName, tableDescriptor);
            }

            private YdbTableHint extractHint() {
                try {
                    Field ydbTableHintField = tableDescriptor.entityType().getDeclaredField("ydbTableHint");
                    ydbTableHintField.setAccessible(true);
                    return (YdbTableHint) ydbTableHintField.get(null);
                } catch (NoSuchFieldException | IllegalAccessException ignored) {
                    return null;
                }
            }

            @Override
            public void drop() {
                String tableName = tableDescriptor.tableName();
                getSchemaOperations().dropTable(tableName);
                entityClassesByTableName.remove(tableName);
            }

            @Override
            public boolean exists() {
                String tableName = tableDescriptor.tableName();
                boolean exists = getSchemaOperations().hasTable(tableName);
                if (exists) {
                    entityClassesByTableName.put(tableName, tableDescriptor);
                } else {
                    entityClassesByTableName.remove(tableName);
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
