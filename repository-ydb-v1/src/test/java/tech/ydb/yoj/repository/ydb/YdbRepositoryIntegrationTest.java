package tech.ydb.yoj.repository.ydb;

import com.google.protobuf.Any;
import com.yandex.ydb.StatusCodesProtos.StatusIds.StatusCode;
import com.yandex.ydb.core.grpc.YdbHeaders;
import com.yandex.ydb.core.utils.Version;
import com.yandex.ydb.discovery.DiscoveryProtos;
import com.yandex.ydb.discovery.v1.DiscoveryServiceGrpc;
import com.yandex.ydb.scheme.v1.SchemeServiceGrpc;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.stats.SessionPoolStats;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Delegate;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.repository.db.exception.DeadlineExceededException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.test.RepositoryTest;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.ChangefeedEntity;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Supabubble2;
import tech.ydb.yoj.repository.test.sample.model.TtlEntity;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.WithUnflattenableField;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbSessionManager;
import tech.ydb.yoj.repository.ydb.compatibility.YdbSchemaCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.exception.ResultTruncatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityChangeIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityCreateIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityDropIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityNew;
import tech.ydb.yoj.repository.ydb.sample.model.HintAutoPartitioningByLoad;
import tech.ydb.yoj.repository.ydb.sample.model.HintInt64Range;
import tech.ydb.yoj.repository.ydb.sample.model.HintTablePreset;
import tech.ydb.yoj.repository.ydb.sample.model.HintUniform;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;
import tech.ydb.yoj.repository.ydb.table.YdbTable;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;
import tech.ydb.yoj.repository.ydb.yql.YqlView;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;
import static tech.ydb.yoj.repository.db.EntityExpressions.newOrderBuilder;

public class YdbRepositoryIntegrationTest extends RepositoryTest {
    private final IndexedEntity e1 =
            new IndexedEntity(new IndexedEntity.Id("1.0"), "key1.0", "value1.0", "value2.0");
    private final IndexedEntity e2 =
            new IndexedEntity(new IndexedEntity.Id("1.1"), "key1.1", "value1.1", "value2.1");

    @Override
    protected Repository createRepository() {
        Repository repository = super.createRepository();
        repository.schema(NonSerializableEntity.class).create();
        repository.schema(WithUnflattenableField.class).create();
        repository.schema(SubdirEntity.class).create();
        repository.schema(TtlEntity.class).create();
        repository.schema(ChangefeedEntity.class).create();
        repository.schema(Supabubble2.class).create();
        return repository;
    }

    @Override
    protected Repository createTestRepository() {
        return new TestYdbRepository(getProxyServerConfig());
    }

    @SneakyThrows
    private YdbConfig getProxyServerConfig() {
        var config = getRealYdbConfig();
        Metadata proxyHeaders = new Metadata();
        proxyHeaders.put(YdbHeaders.DATABASE, config.getDatabase());
        proxyHeaders.put(YdbHeaders.BUILD_INFO, Version.getVersion().get());

        var hostAndPort = config.getEndpoints().get(0);
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
                .maxInboundMessageSize(50000000)
                .intercept(MetadataUtils.newAttachHeadersInterceptor(proxyHeaders));
        ManagedChannel channel;
        if (config.isUseTLS()) {
            channel = channelBuilder.sslContext(GrpcSslContexts.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build()).build();
        } else {
            channel = channelBuilder
                    .usePlaintext()
                    .build();
        }
        ProxyDiscoveryService proxyDiscoveryService = new ProxyDiscoveryService(channel);
        Server proxyServer = NettyServerBuilder.forPort(0)
                .addService(new ProxyYdbTableService(channel))
                .addService(proxyDiscoveryService)
                .addService(new DelegateSchemeServiceImplBase(SchemeServiceGrpc.newStub(channel)))
                .build();
        proxyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(proxyServer::shutdown));

        int port = proxyServer.getPort();
        proxyDiscoveryService.setPort(port);
        return YdbConfig.createForTesting("localhost", proxyServer.getPort(), config.getTablespace(), config.getDatabase())
                .withDiscoveryEndpoint("localhost:" + port);
    }

    protected YdbConfig getRealYdbConfig() {
        return TestYdbConfig.get();
    }

    @Test
    @Ignore
    public void cleanupDB() {
        YdbConfig cfg = getRealYdbConfig();
        var hostAndPort = cfg.getEndpoints().get(0);
        new TestYdbRepository(YdbConfig.createForTesting(
                hostAndPort.getHost(),
                hostAndPort.getPort(),
                "/local/ycloud/",
                null
        )).dropDb();
    }

    @Test
    public void useClosedReadTableStream() {
        db.tx(() -> {
            db.projects().save(new Project(new Project.Id("1"), "p1"));
            db.projects().save(new Project(new Project.Id("2"), "p2"));

        });

        ReadTableParams<Project.Id> params = ReadTableParams.<Project.Id>builder().useNewSpliterator(true).build();
        Stream<Project> readOnlyStream = db.readOnly().run(() -> db.projects().readTable(params));

        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() ->
                readOnlyStream.forEach(System.err::println)
        );
    }

    @Test
    public void throwConversionExceptionOnSerializationProblem() {
        NonSerializableEntity nonSerializableEntity = new NonSerializableEntity(
                new NonSerializableEntity.Id("ru-vladimirsky-central-002"),
                new NonSerializableObject()
        );

        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() -> db.tx(() -> db.table(NonSerializableEntity.class).insert(nonSerializableEntity)));
    }

    @Test
    public void readYqlListAndMap() {
        WithUnflattenableField entity = new WithUnflattenableField(
                new WithUnflattenableField.Id("id_yql_list"),
                new WithUnflattenableField.Unflattenable("Hello, world!", 100_500)
        );
        db.tx(() -> db.table(WithUnflattenableField.class).insert(entity));
        db.tx(() -> {
            List<GroupByResult> result = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction())
                    .execute(new YqlStatement<>(EntitySchema.of(WithUnflattenableField.class),
                            ObjectSchema.of(GroupByResult.class)) {
                        @Override
                        public String getQuery(String tablespace) {
                            return "PRAGMA TablePathPrefix = \"" + tablespace + "\";" +
                                    "select id, AGGREGATE_LIST(id) as items, " +
                                    "AsDict(AsTuple('name',SOME(id))) as map, " +
                                    "AsDict(AsTuple('name',SOME(id))) as `struct`" +
                                    " from WithUnflattenableField group by id";
                        }

                        @Override
                        public String toDebugString(Object o) {
                            return null;
                        }

                        @Override
                        public QueryType getQueryType() {
                            return QueryType.SELECT;
                        }
                    }, null);
            assertEquals(List.of(new GroupByResult("id_yql_list", List.of("id_yql_list"),
                    Map.of("name", "id_yql_list"),
                    new GroupByResult.Struct("id_yql_list"))), result);
        });
    }

    @Value
    static class GroupByResult {
        String id;
        List<String> items;
        Map<String, String> map;
        @Column(flatten = false)
        Struct struct;

        @Value
        static class Struct {
            String name;
        }
    }

    @Test
    public void readViewFromCache() {
        TypeFreak tf1 = newTypeFreak(0, "AAA1", "bbb");
        db.tx(() -> db.typeFreaks().insert(tf1));

        db.tx(() -> {
            TypeFreak.View foundView1 = db.typeFreaks().find(TypeFreak.View.class, tf1.getId());
            TypeFreak.View foundView2 = db.typeFreaks().find(TypeFreak.View.class, tf1.getId());
            // should be the same object, because of cache
            assertThat(foundView2).isSameAs(foundView1);
        });
    }

    @Test
    public void scanMoreThenMaxSize() {
        db.tx(() -> {
            db.projects().save(new Project(new Project.Id("1"), "p1"));
            db.projects().save(new Project(new Project.Id("2"), "p2"));
        });
        Assertions.assertThatExceptionOfType(YdbRepositoryException.class)
                .isThrownBy(() -> db.scan().withMaxSize(1).run(() -> {
                    db.projects().findAll();
                }))
                .satisfies(e -> Assert.assertTrue(e.getCause() instanceof ResultTruncatedException));
    }

    @Test
    public void transactionLevel() {
        Project expected = new Project(new Project.Id("RO"), "readonly");

        TestYdbRepository repository = new TestYdbRepository(getRealYdbConfig());
        SessionManager sessionManager = repository.getSessionManager();
        TestDb db = new TestDbImpl<>(this.repository);

        Session firstSession = sessionManager.getSession();
        sessionManager.release(firstSession);

        db.tx(() -> db.projects().save(expected));

        // Reuse the same session in RO transaction
        checkSession(sessionManager, firstSession);
        Project actual = db.readOnly().run(() -> db.projects().find(expected.getId()));

        assertThat(actual).isEqualTo(expected);

        // Reuse the same session in RW transaction
        checkSession(sessionManager, firstSession);
        actual = db.tx(() -> db.projects().find(expected.getId()));

        assertThat(actual).isEqualTo(expected);
        checkSession(sessionManager, firstSession);
    }

    @SneakyThrows
    @Test
    public void truncated() {
        int maxPageSizeBiggerThatReal = 100_000;
        ListRequest.Builder<Project> builder = ListRequest.builder(Project.class);
        { // because we can't set pageSize bigger than 1k - we set it with reflection
            Field pageSizeField = builder.getClass().getDeclaredField("pageSize");
            pageSizeField.setAccessible(true);
            pageSizeField.set(builder, maxPageSizeBiggerThatReal);
        }
        ListRequest<Project> bigListRequest = builder.build();
        ListRequest<Project> smallListRequest = ListRequest.builder(Project.class).pageSize(100).build();
        db.tx(() -> db.projects().save(new Project(new Project.Id("id"), "name")));

        db.tx(() -> db.projects().list(smallListRequest));
        db.tx(() -> db.projects().list(bigListRequest));
        db.tx(() -> db.projects().findAll());

        db.tx(() -> IntStream.range(0, maxPageSizeBiggerThatReal)
                .forEach(i -> db.projects().save(new Project(new Project.Id("id_" + i), "name"))));

        db.tx(() -> db.projects().list(smallListRequest));
        assertThatExceptionOfType(ResultTruncatedException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().list(bigListRequest)));
        assertThatExceptionOfType(ResultTruncatedException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().findAll()));
    }

    private static void checkSession(SessionManager sessionManager, Session firstSession) {
        Session session = sessionManager.getSession();
        assertThat(session).isEqualTo(firstSession);
        sessionManager.release(session);
    }


    @Test
    public void checkDBIsUnavailable() {
        checkTxRetryableOnRequestError(StatusCode.UNAVAILABLE);
        checkTxRetryableOnFlushingError(StatusCode.UNAVAILABLE);
        checkTxNonRetryableOnCommit(StatusCode.UNAVAILABLE);
        checkTxUnavailableOnNormalRollback(StatusCode.UNAVAILABLE);
    }

    @Test
    public void checkDBIsOverloaded() {
        checkTxRetryableOnRequestError(StatusCode.OVERLOADED);
        checkTxRetryableOnFlushingError(StatusCode.OVERLOADED);
        checkTxNonRetryableOnCommit(StatusCode.OVERLOADED);
        checkTxUnavailableOnNormalRollback(StatusCode.OVERLOADED);
    }

    @Test
    public void checkDBSessionBusy() {
        checkTxRetryableOnRequestError(StatusCode.PRECONDITION_FAILED);
        checkTxRetryableOnFlushingError(StatusCode.PRECONDITION_FAILED);
        checkTxNonRetryableOnCommit(StatusCode.PRECONDITION_FAILED);
        checkTxUnavailableOnNormalRollback(StatusCode.PRECONDITION_FAILED);

        checkTxRetryableOnRequestError(StatusCode.SESSION_BUSY);
        checkTxRetryableOnFlushingError(StatusCode.SESSION_BUSY);
        checkTxNonRetryableOnCommit(StatusCode.SESSION_BUSY);
        checkTxUnavailableOnNormalRollback(StatusCode.SESSION_BUSY);
    }

    @Test
    public void subdirTable() {
        assertThat(((YdbRepository) repository).getSchemaOperations().getTableNames(true))
                .contains("subdir/SubdirEntity");
    }

    @Test
    @Ignore("Flaky test")
    public void sessionsNotLeak() throws InterruptedException {
        long sessionGetTimeout = ((YdbRepository) repository).getConfig().getSessionCreationTimeout().toMillis();
        YdbSessionManager sessionManager = (YdbSessionManager) ((YdbRepository) repository).getSessionManager();
        TableClient sessionClient = sessionManager.getTableClient();

        int sessionPoolSize = sessionClient.getSessionPoolStats().getMaxSize();
        assertThat(sessionClient.getSessionPoolStats().getAcquiredCount())
                .isEqualTo(0);

        List<Session> sessions = IntStream.range(0, sessionPoolSize)
                .mapToObj(i -> sessionManager.getSession())
                .collect(toList());
        assertThat(sessionClient.getSessionPoolStats().getAcquiredCount())
                .isEqualTo(sessionPoolSize);
        assertThat(sessionClient.getSessionPoolStats().getIdleCount())
                .isEqualTo(0);

        Session[] secondSessions = new Session[sessionPoolSize];
        for (int i = 0; i < sessionPoolSize; i++) {
            int finalI = i;
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    secondSessions[finalI] = sessionManager.getSession();
                } catch (Exception ignored) {
                }
            });
        }

        Thread.sleep(sessionGetTimeout / 2); // after that sessions already pended
        assertThat(sessionClient.getSessionPoolStats().getPendingAcquireCount())
                .isEqualTo(sessionPoolSize);

        Thread.sleep(sessionGetTimeout / 2);// deadline was reached

        sessions.forEach(sessionManager::release);
        ForkJoinPool.commonPool().awaitTermination(5, TimeUnit.SECONDS);

        SessionPoolStats stats = sessionClient.getSessionPoolStats();
        long acquiredSessions = Arrays.stream(secondSessions).filter(Objects::nonNull).count();

        assertThat(stats.getAcquiredCount()).isEqualTo(acquiredSessions);
        assertThat(stats.getIdleCount()).isEqualTo(sessionPoolSize - acquiredSessions);
        assertThat(stats.getPendingAcquireCount()).isEqualTo(0);

        Arrays.stream(secondSessions).filter(Objects::nonNull)
                .forEach(sessionManager::release);
    }

    @Test
    public void usesTupleAsParamTest() {
        var id1 = new Bubble.Id("a", "b");
        var id2 = new Bubble.Id("b", "c");
        var id3 = new Bubble.Id("b", "a");

        db.tx(() -> {
            db.bubbles().insert(new Bubble(id1, "oldA", "oldB", "oldC"));
            db.bubbles().insert(new Bubble(id2, "oldA", "oldB", "oldC"));
            db.bubbles().insert(new Bubble(id3, "oldA", "oldB", "oldC"));
        });

        db.tx(() -> this.db.bubbles().updateSomeFields(Set.of(id1, id2), "newA", "newB"));

        db.tx(() -> {
            var first = this.db.bubbles().find(id1);
            var second = this.db.bubbles().find(id2);
            var third = this.db.bubbles().find(id3);

            assertThat(first.getFieldA()).isEqualTo("newA");
            assertThat(first.getFieldB()).isEqualTo("newB");
            assertThat(first.getFieldC()).isEqualTo("oldC");

            assertThat(second.getFieldA()).isEqualTo("newA");
            assertThat(second.getFieldB()).isEqualTo("newB");
            assertThat(second.getFieldC()).isEqualTo("oldC");

            assertThat(third.getFieldA()).isEqualTo("oldA");
            assertThat(third.getFieldB()).isEqualTo("oldB");
            assertThat(third.getFieldC()).isEqualTo("oldC");
        });
    }

    @Test
    public void updateInSingleKey() {
        var id1 = new IndexedEntity.Id("1");
        var id2 = new IndexedEntity.Id("2");
        db.tx(() -> {
            db.indexedTable().insert(new IndexedEntity(id1, "11", "111", "1111"));
            db.indexedTable().insert(new IndexedEntity(id2, "22", "222", "2222"));
        });

        db.tx(() -> db.indexedTable().updateSomeFields(Set.of(id1, id2), "4", "5"));
        db.tx(() -> {
            var v1 = this.db.indexedTable().find(id1);
            var v2 = this.db.indexedTable().find(id2);

            assertThat(v1).isEqualTo(new IndexedEntity(id1, "11", "4", "5"));
            assertThat(v2).isEqualTo(new IndexedEntity(id2, "22", "4", "5"));
        });
    }

    @Test
    public void predicateWithBoxedValues() {
        var p = new Project(new Project.Id("abcdefg"), "hijklmnop");
        db.tx(() -> {
            db.projects().save(p);
        });
        db.tx(() -> {
            var table = (YdbTable<Project>) db.table(Project.class);
            var res = table.find(YqlPredicate.where("id").eq(p.getId()));
            assertThat(res)
                    .hasSize(1)
                    .contains(p);
        });
    }

    @Test
    public void testSelectDefault() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_version_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` " +
                        "WHERE `version_id` = $pred_0_version_id " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("id.versionId").eq("1.1")));
    }

    @Test
    public void testSelectIndex1Default() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_key_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` " +
                        "WHERE `key_id` = $pred_0_key_id " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("keyId").eq("key1.1")));
    }

    @Test
    public void testSelectIndex1WithoutFields() {
        // No exception at this point, but should be?

        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_version_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `key_index` " +
                        "WHERE `version_id` = $pred_0_version_id " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("id.versionId").eq("1.1"),
                        YqlView.index("key_index")));
    }

    @Test
    public void testSelectIndex1WithEmptyIndex() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_key_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes`  " +
                        "WHERE `key_id` = $pred_0_key_id ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("keyId").eq("key1.1"),
                        YqlView.empty()));
    }

    @Test
    public void testSelectIndex1WithIndex() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_key_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `key_index` " +
                        "WHERE `key_id` = $pred_0_key_id " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("keyId").eq("key1.1"),
                        YqlView.index("key_index")));
    }

    @Test
    public void testSelectIndex2Default() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_value_id AS String;\n" +
                        "DECLARE $pred_1_valueId2 AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` " +
                        "WHERE (`value_id` = $pred_0_value_id) AND (`valueId2` = $pred_1_valueId2) " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("valueId").eq("value1.1").and("valueId2").eq("value2.1")));
    }

    @Test
    public void testSelectIndex2WithIndex() {
        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_value_id AS String;\n" +
                        "DECLARE $pred_1_valueId2 AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `value_index` " +
                        "WHERE (`value_id` = $pred_0_value_id) AND (`valueId2` = $pred_1_valueId2) " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("valueId").eq("value1.1").and("valueId2").eq("value2.1"),
                        YqlView.index("value_index")));
    }

    @Test
    public void testSelectIndex2WithFirstFieldOnly() {
        // No exception at this point, but should be?

        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_value_id AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `value_index` " +
                        "WHERE `value_id` = $pred_0_value_id " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("valueId").eq("value1.1"),
                        YqlView.index("value_index")));
    }

    @Test
    public void testSelectIndex2WithSecondFieldOnly() {
        // No exception at this point, but should be?

        db.tx(() -> db.indexedTable().insert(e1, e2));
        executeQuery("DECLARE $pred_0_valueId2 AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `value_index` " +
                        "WHERE `valueId2` = $pred_0_valueId2 " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                List.of(YqlPredicate.where("valueId2").eq("value2.1"),
                        YqlView.index("value_index")));
    }

    @Test
    public void testBuildStatementPartsWithGlobalIndex() {
        db.tx(() -> db.indexedTable().insert(e1, e2));

        var filter = newFilterBuilder(IndexedEntity.class)
                .where("valueId2").eq("value2.1")
                .build();

        var orderBy = newOrderBuilder(IndexedEntity.class)
                .orderBy("id.versionId")
                .ascending()
                .build();

        var statements = YdbTable.buildStatementParts("value_index", filter, orderBy, null, null);

        executeQuery("DECLARE $pred_0_valueId2 AS String;\n" +
                        "SELECT `version_id`, `key_id`, `value_id`, `valueId2` " +
                        "FROM `ts/table_with_indexes` VIEW `value_index` " +
                        "WHERE `valueId2` = $pred_0_valueId2 " +
                        "ORDER BY `version_id` ASC",
                List.of(e2),
                statements);
    }

    @Test
    public void complexInPredicate() {
        var bubbles = new ArrayList<Bubble>();
        for (int i = 0; i < 20; i++) {
            bubbles.add(new Bubble(
                    new Bubble.Id("k1_" + (i / 10), "k2_" + (i % 10)),
                    "v1_" + i, "v2_" + i, "v3_" + i
            ));
        }
        var searchingBubbles = bubbles.subList(0, bubbles.size() / 2);
        db.tx(() -> db.bubbles().insertAll(bubbles));

        var searchingIds = searchingBubbles.stream()
                .map(Bubble::getId)
                .collect(Collectors.toList());
        db.tx(() -> {
            var table = (YdbTable<Bubble>) db.bubbles();
            var foundBubbles = table.find(
                    YqlPredicate.where("id").in(searchingIds)
            );
            assertThat(foundBubbles).isEqualTo(searchingBubbles);
        });
    }

    @Test
    public void bulkInserts() {
        var id1 = new Bubble.Id("a", "b");
        var id2 = new Bubble.Id("c", "d");

        db.tx(() -> {
            db.bubbles().bulkUpsert(
                    List.of(new Bubble(id1, "oldA", "oldB", "oldC"), new Bubble(id2, "oldA", "oldB", "oldC")),
                    BulkParams.DEFAULT
            );
        });

        db.readOnly().run(() -> {
            var first = this.db.bubbles().find(id1);
            assertThat(first).isNotNull();
            assertThat(first.getFieldA()).isEqualTo("oldA");
            assertThat(this.db.bubbles().find(id2)).isNotNull();
        });
    }

    @Test
    @Ignore("Flaky test with bad assumptions")
    public void checkThatOperationTimesOutIfGrpsDeadlineIsSet() throws InterruptedException {
        final long timeoutMillis = 60;
        long longOperationTimeout = 200;
        long expectedNumberOfProjects = 10_000;

        var projects = new ArrayList<Project>();
        for (var i = 0; i < expectedNumberOfProjects; i++) {
            String projectId = String.valueOf(i);
            projects.add(new Project(new Project.Id(projectId), projectId));
        }

        db.tx(() -> {
            db.projects().bulkUpsert(projects, BulkParams.DEFAULT);
        });

        assertThatExceptionOfType(DeadlineExceededException.class).isThrownBy(() -> {
            var executor = Executors.newSingleThreadScheduledExecutor();
            io.grpc.Context.current().withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS, executor).run(() -> {
                db.tx(() -> {
                    db.projects().deleteAll();
                });
            });
        });

        //wait for operation to complete, in case in wasn't cancelled
        Thread.sleep(longOperationTimeout);

        //check that operation is cancelled and changes aren't applied
        long actualNumberOfProjects = db.tx(() -> {
            return db.projects().countAll();
        });
        assertThat(actualNumberOfProjects).isEqualTo(expectedNumberOfProjects);
    }

    @Test
    public void testTransactionTakesTimeoutFromGrpcContext() {
        int[] timeoutsMin = IntStream.range(3, 12).toArray();

        for (var timeout : timeoutsMin) {
            testTransactionTakesTimeoutFromGrpcContext(timeout);
        }
    }

    private void testTransactionTakesTimeoutFromGrpcContext(int timeoutMin) {
        db.withTimeout(Duration.ofMinutes(timeoutMin)).tx(() -> {
            RepositoryTransaction transaction = Tx.Current.get().getRepositoryTransaction();
            var testTransaction = (TestYdbRepository.TestYdbRepositoryTransaction) transaction;
            var actualTimeout = testTransaction.getOptions().getTimeoutOptions().getTimeout();
            assertThat(actualTimeout.toMinutes()).isEqualTo(timeoutMin);
        });
    }

    @Test
    public void testCompatibilityDropIndex() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(IndexedEntityDropIndex.class), (YdbRepository) repository);
        checker.run();
        Assertions.assertThat(checker.getShouldExecuteMessages()).isEmpty();

        var ts = getRealYdbConfig().getTablespace();
        Assertions.assertThat(checker.getCanExecuteMessages()).containsAnyOf(
                String.format("ALTER TABLE `%stable_with_indexes` DROP INDEX `key_index`;", ts)
        );
    }

    @Test
    public void testCompatibilityCreateIndex() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(IndexedEntityCreateIndex.class), (YdbRepository) repository);
        Assertions.assertThatThrownBy(checker::run);
        var ts = getRealYdbConfig().getTablespace();
        Assertions.assertThat(checker.getShouldExecuteMessages()).containsExactly(
                String.format("ALTER TABLE `%stable_with_indexes` ADD INDEX `key2_index` GLOBAL ON (`key_id`,`valueId2`);", ts)
        );
    }

    @Test
    public void testCompatibilityNewIndexedTable() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(IndexedEntityNew.class), (YdbRepository) repository);
        Assertions.assertThatThrownBy(checker::run);
        var ts = getRealYdbConfig().getTablespace();
        String expected = String.format(
                "CREATE TABLE `%snew_table_with_indexes` (\n" +
                        "\t`version_id` STRING,\n" +
                        "\t`key_id` STRING,\n" +
                        "\t`value_id` STRING,\n" +
                        "\t`valueId2` STRING,\n" +
                        "\tPRIMARY KEY(`version_id`),\n" +
                        "\tINDEX `key_index` GLOBAL ON (`key_id`),\n" +
                        "\tINDEX `value_index` GLOBAL ON (`value_id`,`valueId2`),\n" +
                        "\tINDEX `key2_index` GLOBAL ON (`key_id`,`valueId2`)\n" +
                        ");",
                ts);
        Assert.assertEquals(expected, checker.getShouldExecuteMessages().get(0));
        Assertions.assertThat(checker.getShouldExecuteMessages()).containsExactly(
                expected
        );

    }

    @Test
    public void testCompatibilityChangeIndex() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(IndexedEntityChangeIndex.class), (YdbRepository) repository);
        Assertions.assertThatThrownBy(checker::run);

        var ts = getRealYdbConfig().getTablespace();
        String message = String.format("Altering index `%stable_with_indexes`.value_index is impossible: " +
                "columns are changed: [value_id, valueId2] --> [value_id].\n", ts);
        message += String.format("ALTER TABLE `%stable_with_indexes` DROP INDEX `value_index`;\n", ts);
        message += String.format("ALTER TABLE `%stable_with_indexes` ADD INDEX `value_index` GLOBAL ON (`value_id`);", ts);
        Assertions.assertThat(checker.getShouldExecuteMessages()).containsExactly(message);
    }

    @Test
    public void complexIdLtYsingYqlPredicate() {
        Supabubble2 sa = new Supabubble2(new Supabubble2.Id(new Project.Id("naher"), "bubble-A"));
        Supabubble2 sb = new Supabubble2(new Supabubble2.Id(new Project.Id("naher"), "bubble-B"));
        Supabubble2 sc = new Supabubble2(new Supabubble2.Id(new Project.Id("naher"), "bubble-C"));
        db.tx(() -> db.supabubbles2().insert(sa, sb, sc));

        assertThat(db.tx(() -> db.supabubbles2().findLessThan(sc.getId()))).containsOnly(sa, sb);
    }

    private void executeQuery(String expectSqlQuery, List<IndexedEntity> expectRows,
                              Collection<? extends YqlStatementPart<?>> query) {
        var statement = YqlStatement.find(IndexedEntity.class, query);
        var sqlQuery = statement.getQuery("ts/");
        assertEquals(expectSqlQuery, sqlQuery);

        // Check we use index and query was not failed
        var actual = db.tx(() -> ((YdbTable<IndexedEntity>) db.indexedTable()).find(query));
        assertEquals(expectRows, actual);
    }

    private void checkTxRetryableOnRequestError(StatusCode statusCode) {
        RepositoryTransaction tx = repository.startTransaction();
        runWithModifiedStatusCode(
                statusCode,
                () -> {
                    assertThatExceptionOfType(RetryableException.class)
                            .isThrownBy(tx.table(Project.class)::findAll);

                    // This rollback is only a silent DB rollback, since the last transaction statement was exceptional.
                    // We check that this call does not throw.
                    tx.rollback();
                }
        );
    }

    private void checkTxRetryableOnFlushingError(StatusCode statusCode) {
        runWithModifiedStatusCode(
                statusCode,
                () -> {
                    RepositoryTransaction tx = repository.startTransaction();
                    tx.table(Project.class).save(new Project(new Project.Id("1"), "x"));
                    assertThatExceptionOfType(RetryableException.class)
                            .isThrownBy(tx::commit);
                }
        );
    }

    private void checkTxNonRetryableOnCommit(StatusCode statusCode) {
        RepositoryTransaction tx = repository.startTransaction();
        tx.table(Project.class).findAll();

        runWithModifiedStatusCode(
                statusCode,
                () -> assertThatExceptionOfType(UnavailableException.class)
                        .isThrownBy(tx::commit)
        );
    }

    private void checkTxUnavailableOnNormalRollback(StatusCode statusCode) {
        RepositoryTransaction tx = repository.startTransaction();
        tx.table(Project.class).findAll();

        // This rollback is a checking consistency DB commit, since the last transaction statement finished normally.
        runWithModifiedStatusCode(
                statusCode,
                () -> assertThatExceptionOfType(UnavailableException.class)
                        .isThrownBy(tx::rollback)
        );
    }

    static StatusCode statusCode = null;

    private void runWithModifiedStatusCode(StatusCode code, Runnable runnable) {
        statusCode = code;
        try {
            runnable.run();
        } finally {
            statusCode = null;
        }
    }

    @Test
    public void schemaWithHint() {
        repository.schema(HintInt64Range.class).create();
        repository.schema(HintUniform.class).create();
        repository.schema(HintTablePreset.class).create();
        repository.schema(HintAutoPartitioningByLoad.class).create();
    }

    @AllArgsConstructor
    private static class DelegateSchemeServiceImplBase extends SchemeServiceGrpc.SchemeServiceImplBase {
        @Delegate
        final SchemeServiceGrpc.SchemeServiceStub schemeServiceStub;
    }

    private static class ProxyDiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase {
        @Delegate(excludes = ProxyDiscoveryService.OverriddenMethod.class)
        DiscoveryServiceGrpc.DiscoveryServiceStub stub;
        @Setter
        int port;

        ProxyDiscoveryService(ManagedChannel channel) {
            stub = DiscoveryServiceGrpc.newStub(channel);
        }

        @Override
        public void listEndpoints(DiscoveryProtos.ListEndpointsRequest request, StreamObserver<DiscoveryProtos.ListEndpointsResponse> responseObserver) {
            stub.listEndpoints(request, new ProxyDiscoveryService.DelegateStreamObserver<>(responseObserver) {
                @Override
                @SneakyThrows
                public void onNext(DiscoveryProtos.ListEndpointsResponse response) {
                    DiscoveryProtos.ListEndpointsResult endpoints = response.getOperation().getResult()
                            .unpack(DiscoveryProtos.ListEndpointsResult.class);
                    Any result = Any.pack(endpoints.toBuilder()
                            .setEndpoints(0, DiscoveryProtos.EndpointInfo.newBuilder()
                                    .setAddress("localhost")
                                    .setPort(port)
                                    .build())
                            .build());
                    super.onNext(response.toBuilder()
                            .setOperation(response.getOperation().toBuilder().setResult(result))
                            .build());
                }
            });
        }

        @AllArgsConstructor
        private abstract static class DelegateStreamObserver<V> implements StreamObserver<V> {
            @Delegate
            StreamObserver<V> responseObserver;
        }

        private interface OverriddenMethod {
            void listEndpoints(DiscoveryProtos.ListEndpointsRequest request, StreamObserver<DiscoveryProtos.ListEndpointsResponse> responseObserver);
        }
    }

    private static class ProxyYdbTableService extends com.yandex.ydb.table.v1.TableServiceGrpc.TableServiceImplBase {
        @Delegate(excludes = ProxyYdbTableService.OverriddenMethod.class)
        com.yandex.ydb.table.v1.TableServiceGrpc.TableServiceStub tableServiceStub;

        ProxyYdbTableService(ManagedChannel channel) {
            tableServiceStub = com.yandex.ydb.table.v1.TableServiceGrpc.newStub(channel);
        }

        @Override
        public void executeDataQuery(com.yandex.ydb.table.YdbTable.ExecuteDataQueryRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.ExecuteDataQueryResponse> responseObserver) {
            tableServiceStub.executeDataQuery(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(com.yandex.ydb.table.YdbTable.ExecuteDataQueryResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        @Override
        public void commitTransaction(com.yandex.ydb.table.YdbTable.CommitTransactionRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.CommitTransactionResponse> responseObserver) {
            tableServiceStub.commitTransaction(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(com.yandex.ydb.table.YdbTable.CommitTransactionResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        @Override
        public void rollbackTransaction(com.yandex.ydb.table.YdbTable.RollbackTransactionRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.RollbackTransactionResponse> responseObserver) {
            tableServiceStub.rollbackTransaction(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(com.yandex.ydb.table.YdbTable.RollbackTransactionResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        private com.yandex.ydb.OperationProtos.Operation breakOperation(com.yandex.ydb.OperationProtos.Operation operation) {
            if (statusCode != null) {
                return operation.toBuilder().setStatus(statusCode).build();
            } else {
                return operation;
            }
        }

        @AllArgsConstructor
        private abstract static class DelegateStreamObserver<V> implements StreamObserver<V> {
            @Delegate
            StreamObserver<V> responseObserver;
        }

        private interface OverriddenMethod {
            void executeDataQuery(com.yandex.ydb.table.YdbTable.ExecuteDataQueryRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.ExecuteDataQueryResponse> responseObserver);

            void commitTransaction(com.yandex.ydb.table.YdbTable.CommitTransactionRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.CommitTransactionResponse> responseObserver);

            void rollbackTransaction(com.yandex.ydb.table.YdbTable.RollbackTransactionRequest request, StreamObserver<com.yandex.ydb.table.YdbTable.RollbackTransactionResponse> responseObserver);
        }
    }
}
