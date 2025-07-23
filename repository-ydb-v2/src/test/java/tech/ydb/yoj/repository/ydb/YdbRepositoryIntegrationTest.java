package tech.ydb.yoj.repository.ydb;

import com.google.protobuf.Any;
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
import org.junit.ClassRule;
import org.junit.Test;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.YdbHeaders;
import tech.ydb.core.utils.Version;
import tech.ydb.proto.OperationProtos;
import tech.ydb.proto.StatusCodesProtos;
import tech.ydb.proto.discovery.DiscoveryProtos;
import tech.ydb.proto.discovery.v1.DiscoveryServiceGrpc;
import tech.ydb.proto.scheme.v1.SchemeServiceGrpc;
import tech.ydb.proto.table.v1.TableServiceGrpc;
import tech.ydb.proto.topic.v1.TopicServiceGrpc;
import tech.ydb.table.Session;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.QueryStatsMode;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.test.RepositoryTest;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.ChangefeedEntity;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Supabubble2;
import tech.ydb.yoj.repository.test.sample.model.TtlEntity;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.UniqueProject;
import tech.ydb.yoj.repository.test.sample.model.WithUnflattenableField;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.compatibility.YdbSchemaCompatibilityChecker;
import tech.ydb.yoj.repository.ydb.exception.ResultTruncatedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.model.EntityChangeTtl;
import tech.ydb.yoj.repository.ydb.model.EntityDropTtl;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityChangeIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityCreateIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityDropIndex;
import tech.ydb.yoj.repository.ydb.model.IndexedEntityNew;
import tech.ydb.yoj.repository.ydb.sample.model.HintAutoPartitioningByLoad;
import tech.ydb.yoj.repository.ydb.sample.model.HintInt64Range;
import tech.ydb.yoj.repository.ydb.sample.model.HintTablePreset;
import tech.ydb.yoj.repository.ydb.sample.model.HintUniform;
import tech.ydb.yoj.repository.ydb.statement.FindStatement;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;
import tech.ydb.yoj.repository.ydb.table.YdbTable;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;
import tech.ydb.yoj.repository.ydb.yql.YqlView;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.Assert.assertEquals;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;
import static tech.ydb.yoj.repository.db.EntityExpressions.newOrderBuilder;

public class YdbRepositoryIntegrationTest extends RepositoryTest {
    private final IndexedEntity e1 =
            new IndexedEntity(new IndexedEntity.Id("1.0"), "key1.0", "value1.0", "value2.0");
    private final IndexedEntity e2 =
            new IndexedEntity(new IndexedEntity.Id("1.1"), "key1.1", "value1.1", "value2.1");

    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    @Override
    protected Repository createRepository() {
        Repository repository = super.createRepository();
        repository.schema(NonSerializableEntity.class).create();
        repository.schema(WithUnflattenableField.class).create();
        repository.schema(SubdirEntity.class).create();
        repository.schema(TtlEntity.class).create();
        repository.schema(ChangefeedEntity.class).create();
        return repository;
    }

    @Override
    protected Repository createTestRepository() {
        return new TestYdbRepository(getRealYdbConfig(), ydbEnvAndTransport.getGrpcTransport());
    }

    @SneakyThrows
    private YdbConfig getProxyServerConfig() {
        var config = getRealYdbConfig();
        Metadata proxyHeaders = new Metadata();
        proxyHeaders.put(YdbHeaders.DATABASE, config.getDatabase());
        proxyHeaders.put(YdbHeaders.BUILD_INFO, Version.getVersion().get());

        var hostAndPort = config.getHostAndPort();
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
                .addService(new DelegateTopicServiceImplBase(TopicServiceGrpc.newStub(channel)))
                .build();
        proxyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(proxyServer::shutdown));

        int port = proxyServer.getPort();
        proxyDiscoveryService.setPort(port);
        return YdbConfig
                .createForTesting("localhost", proxyServer.getPort(), config.getTablespace(), config.getDatabase())
                .withDiscoveryEndpoint("localhost:" + port);
    }

    protected YdbConfig getRealYdbConfig() {
        return ydbEnvAndTransport.getYdbConfig();
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
            EntitySchema<WithUnflattenableField> schema = EntitySchema.of(WithUnflattenableField.class);
            var tableDescriptor = TableDescriptor.from(schema);
            List<GroupByResult> result = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction())
                    .execute(new YqlStatement<>(tableDescriptor, schema, ObjectSchema.of(GroupByResult.class)) {
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

        SessionManager sessionManager = ((YdbRepository) this.repository).getSessionManager();
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

    @Test
    public void snapshotTransactionLevel() {
        Project expected1 = new Project(new Project.Id("SP1"), "snapshot1");
        Project expected2 = new Project(new Project.Id("SP2"), "snapshot2");

        db.tx(() -> db.projects().save(expected1));
        db.tx(() -> db.projects().save(expected2));

        Project actual1 = db.tx(() -> db.projects().find(expected1.getId()));
        assertThat(actual1).isEqualTo(expected1);
        Project actual2 = db.readOnly().run(() -> db.projects().find(expected2.getId()));
        assertThat(actual2).isEqualTo(expected2);

        db.readOnly()
                .withStatementIsolationLevel(IsolationLevel.SNAPSHOT)
                .run(() -> {
                    Project actualSnapshot1 = db.projects().find(expected1.getId());
                    assertThat(actualSnapshot1).isEqualTo(expected1);

                    Project actualSnapshot2 = db.projects().find(expected2.getId());
                    assertThat(actualSnapshot2).isEqualTo(expected2);
                });
    }

    @Test
    @SneakyThrows
    public void truncated() {
        int maxPageSizeBiggerThatReal = 10_001;
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

    @Test
    public void inSingleElementListOptimizedToEq() {
        Project expected1 = new Project(new Project.Id("SP1"), "snapshot1");
        Project expected2 = new Project(new Project.Id("SP2"), "snapshot2");
        db.tx(() -> db.projects().insert(expected1, expected2));

        List<Project> found = db.tx(() -> db.projects().query()
                .where("id").in(expected1.getId())
                .find());
        assertThat(found).singleElement().isEqualTo(expected1);
    }

    @Test
    public void notInSingleElementListOptimizedToNeq() {
        Project expected1 = new Project(new Project.Id("SP1"), "snapshot1");
        Project expected2 = new Project(new Project.Id("SP2"), "snapshot2");
        db.tx(() -> db.projects().insert(expected1, expected2));

        List<Project> found = db.tx(() -> db.projects().query()
                .where("id").notIn(expected1.getId())
                .find());
        assertThat(found).singleElement().isEqualTo(expected2);
    }

    private static void checkSession(SessionManager sessionManager, Session firstSession) {
        Session session = sessionManager.getSession();
        assertThat(session).isEqualTo(firstSession);
        sessionManager.release(session);
    }


    @Test
    public void checkDBIsUnavailable() {
        checkTxRetryableOnRequestError(StatusCodesProtos.StatusIds.StatusCode.UNAVAILABLE);
        checkTxRetryableOnFlushingError(StatusCodesProtos.StatusIds.StatusCode.UNAVAILABLE);
        checkTxNonRetryableOnCommit(StatusCodesProtos.StatusIds.StatusCode.UNAVAILABLE);
    }

    @Test
    public void checkDBIsOverloaded() {
        checkTxRetryableOnRequestError(StatusCodesProtos.StatusIds.StatusCode.OVERLOADED);
        checkTxRetryableOnFlushingError(StatusCodesProtos.StatusIds.StatusCode.OVERLOADED);
        checkTxNonRetryableOnCommit(StatusCodesProtos.StatusIds.StatusCode.OVERLOADED);
    }

    @Test
    public void checkDBSessionBusy() {
        checkTxRetryableOnRequestError(StatusCodesProtos.StatusIds.StatusCode.PRECONDITION_FAILED);
        checkTxRetryableOnFlushingError(StatusCodesProtos.StatusIds.StatusCode.PRECONDITION_FAILED);
        checkTxNonRetryableOnCommit(StatusCodesProtos.StatusIds.StatusCode.PRECONDITION_FAILED);

        checkTxRetryableOnRequestError(StatusCodesProtos.StatusIds.StatusCode.SESSION_BUSY);
        checkTxRetryableOnFlushingError(StatusCodesProtos.StatusIds.StatusCode.SESSION_BUSY);
        checkTxNonRetryableOnCommit(StatusCodesProtos.StatusIds.StatusCode.SESSION_BUSY);
    }

    @Test
    public void subdirTable() {
        Assertions.assertThat(((YdbRepository) repository).getSchemaOperations().getTableNames(true))
                .contains("subdir/SubdirEntity");
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
    public void predicateWithMultipleBoxedId() {
        var m = new MultiWrappedEntity(new MultiWrappedEntity.Id(new MultiWrappedEntity.StringWrapper("string-id")), "payload", null);
        db.tx(() -> {
            db.multiWrappedIdEntities().save(m);
        });
        db.tx(() -> {
            assertThat(db.multiWrappedIdEntities().query().where("id").eq(m.id()).findOne()).isEqualTo(m);
            assertThat(db.multiWrappedIdEntities().query().where("id").eq(m.id().itIsReallyString()).findOne()).isEqualTo(m);
            assertThat(db.multiWrappedIdEntities().query().where("id").eq(m.id().itIsReallyString().value()).findOne()).isEqualTo(m);

            var table = (YdbTable<MultiWrappedEntity>) db.table(MultiWrappedEntity.class);
            assertThat(table.find(YqlPredicate.where("id").eq(m.id()))).singleElement().isEqualTo(m);
            assertThat(table.find(YqlPredicate.where("id").eq(m.id().itIsReallyString()))).singleElement().isEqualTo(m);
            assertThat(table.find(YqlPredicate.where("id").eq(m.id().itIsReallyString().value()))).singleElement().isEqualTo(m);
        });
    }

    @Test
    public void predicateWithMultipleBoxedPayload() {
        var m = new MultiWrappedEntity(
                new MultiWrappedEntity.Id(new MultiWrappedEntity.StringWrapper("string-id")),
                "fakefakefake",
                new MultiWrappedEntity.OptionalPayload(new MultiWrappedEntity.StringWrapper("real-payload"))
        );
        db.tx(() -> {
            db.multiWrappedIdEntities().save(m);
        });
        db.tx(() -> {
            assertThat(db.multiWrappedIdEntities().query().where("optionalPayload").eq(m.optionalPayload()).findOne()).isEqualTo(m);
            assertThat(db.multiWrappedIdEntities().query().where("optionalPayload").eq(m.optionalPayload().wrapper()).findOne()).isEqualTo(m);
            assertThat(db.multiWrappedIdEntities().query().where("optionalPayload").eq(m.optionalPayload().wrapper().value()).findOne()).isEqualTo(m);

            var table = (YdbTable<MultiWrappedEntity>) db.table(MultiWrappedEntity.class);
            assertThat(table.find(YqlPredicate.where("optionalPayload").eq(m.optionalPayload()))).singleElement().isEqualTo(m);
            assertThat(table.find(YqlPredicate.where("optionalPayload").eq(m.optionalPayload().wrapper()))).singleElement().isEqualTo(m);
            assertThat(table.find(YqlPredicate.where("optionalPayload").eq(m.optionalPayload().wrapper().value()))).singleElement().isEqualTo(m);
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
    public void testCompatibilityDropTtl() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(EntityDropTtl.class), (YdbRepository) repository);
        Assertions.assertThatThrownBy(checker::run);
        var ts = getRealYdbConfig().getTablespace();
        Assertions.assertThat(checker.getShouldExecuteMessages()).containsExactly(
                String.format("ALTER TABLE `%sTtlEntity` RESET (TTL);", ts)
        );
    }

    @Test
    public void testCompatibilityChangeOrCreateTtl() {
        var checker = new YdbSchemaCompatibilityChecker(List.of(EntityChangeTtl.class), (YdbRepository) repository);
        Assertions.assertThatThrownBy(checker::run);
        var ts = getRealYdbConfig().getTablespace();
        Assertions.assertThat(checker.getShouldExecuteMessages()).containsExactly(
                String.format("ALTER TABLE `%sTtlEntity` SET (TTL = Interval(\"PT2H\") ON createdAt);", ts)
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
                        "\tINDEX `key2_index` GLOBAL ON (`key_id`,`valueId2`),\n" +
                        "\tINDEX `key3_index` GLOBAL ASYNC ON (`key_id`,`value_id`)\n" +
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
        message += String.format("ALTER TABLE `%stable_with_indexes` ADD INDEX `value_index` GLOBAL ON (`value_id`);\n", ts);
        message += String.format("ALTER TABLE `%stable_with_indexes` ADD INDEX `value_index2` GLOBAL ASYNC ON (`value_id`);", ts);
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

    private void executeQuery(String expectSqlQuery, List<IndexedEntity> expectRows, List<YqlStatementPart<?>> parts) {
        EntitySchema<IndexedEntity> schema = EntitySchema.of(IndexedEntity.class);
        TableDescriptor<IndexedEntity> tableDescriptor = TableDescriptor.from(schema);
        var statement = FindStatement.from(
                tableDescriptor, schema, schema, parts, false
        );
        var sqlQuery = statement.getQuery("ts/");
        assertEquals(expectSqlQuery, sqlQuery);

        // Check we use index and query was not failed
        var actual = db.tx(() -> ((YdbTable<IndexedEntity>) db.indexedTable()).find(parts));
        assertEquals(expectRows, actual);
    }

    private void checkTxRetryableOnRequestError(StatusCodesProtos.StatusIds.StatusCode statusCode) {
        YdbRepository proxiedRepository = new YdbRepository(getProxyServerConfig());

        try {
            RepositoryTransaction tx = proxiedRepository.startTransaction();
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
        } finally {
            proxiedRepository.shutdown();
        }
    }

    private void checkTxRetryableOnFlushingError(StatusCodesProtos.StatusIds.StatusCode statusCode) {
        YdbRepository proxiedRepository = new YdbRepository(getProxyServerConfig());

        try {
            runWithModifiedStatusCode(
                    statusCode,
                    () -> {
                        RepositoryTransaction tx = proxiedRepository.startTransaction();
                        tx.table(Project.class).save(new Project(new Project.Id("1"), "x"));
                        assertThatExceptionOfType(RetryableException.class)
                                .isThrownBy(tx::commit);
                    }
            );
        } finally {
            proxiedRepository.shutdown();
        }
    }

    private void checkTxNonRetryableOnCommit(StatusCodesProtos.StatusIds.StatusCode statusCode) {
        YdbRepository proxiedRepository = new YdbRepository(getProxyServerConfig());

        try {
            RepositoryTransaction tx = proxiedRepository.startTransaction();
            tx.table(Project.class).findAll();

            runWithModifiedStatusCode(
                    statusCode,
                    () -> assertThatExceptionOfType(UnavailableException.class)
                            .isThrownBy(tx::commit)
            );
        } finally {
            proxiedRepository.shutdown();
        }
    }

    static StatusCodesProtos.StatusIds.StatusCode statusCode = null;

    private void runWithModifiedStatusCode(StatusCodesProtos.StatusIds.StatusCode code, Runnable runnable) {
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

    @Test
    public void creatingRepositoryDoesNotConnect() {
        YdbConfig intentionallyBadConfig = YdbConfig
                .createForTesting("must-not-connect", 44444, "/nothing/here/", "/nothing")
                // This forces YDB SDK to connect at GrpcTransport creation
                .withUseSingleChannelTransport(false);

        YdbRepository repository = new TestYdbRepository(intentionallyBadConfig);
        repository.shutdown();
    }

    @Test
    public void ydbTransactionCompatibility() {
        db.tx(() -> {
            // No db tx or session yet!
            var sdkTx = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction()).toSdkTransaction();
            assertThatIllegalStateException().isThrownBy(sdkTx::getSessionId);
            assertThat(sdkTx.getId()).isNull();
            assertThat(sdkTx.getTxMode()).isEqualTo(TxMode.SERIALIZABLE_RW);
            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(sdkTx::getStatusFuture);

            // Perform any read - session and tx ID appear
            db.projects().countAll();
            sdkTx = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction()).toSdkTransaction();
            assertThat(sdkTx.getSessionId()).isNotNull();
            assertThat(sdkTx.getId()).isNotNull();
            assertThat(sdkTx.getTxMode()).isEqualTo(TxMode.SERIALIZABLE_RW);
            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(sdkTx::getStatusFuture);
        });

        for (var entry : Map.of(
                IsolationLevel.ONLINE_CONSISTENT_READ_ONLY, TxMode.ONLINE_RO,
                IsolationLevel.ONLINE_INCONSISTENT_READ_ONLY, TxMode.ONLINE_INCONSISTENT_RO,
                IsolationLevel.STALE_CONSISTENT_READ_ONLY, TxMode.STALE_RO,
                IsolationLevel.SNAPSHOT, TxMode.SNAPSHOT_RO
        ).entrySet()) {
            var isolationLevel = entry.getKey();
            var txMode = entry.getValue();

            db.readOnly().withStatementIsolationLevel(isolationLevel).run(() -> {
                // No db tx or session yet!
                var sdkTx = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction()).toSdkTransaction();
                assertThatIllegalStateException().isThrownBy(sdkTx::getSessionId);
                assertThat(sdkTx.getId()).isNull();
                assertThat(sdkTx.getTxMode()).isEqualTo(txMode);
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(sdkTx::getStatusFuture);

                // Perform any read - session and tx ID appear
                db.projects().countAll();
                sdkTx = ((YdbRepositoryTransaction<?>) Tx.Current.get().getRepositoryTransaction()).toSdkTransaction();
                assertThat(sdkTx.getSessionId()).isNotNull();
                // Read transactions might have no ID or might have an ID, depending on your YDB version (that's what YDB returns, folks!)
                assertThat(sdkTx.getTxMode()).isEqualTo(txMode);
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(sdkTx::getStatusFuture);
            });
        }
    }

    @Test
    public void unordered() {
        // YDB tends to return data in index-order, not "by PK ascending" order, if we don't force the result order
        IndexedEntity ie1 = new IndexedEntity(new IndexedEntity.Id("abc"), "z", "v1-1", "v1-2");
        IndexedEntity ie2 = new IndexedEntity(new IndexedEntity.Id("def"), "y", "v2-1", "v2-2");
        db.tx(() -> db.indexedTable().insert(ie1, ie2));

        var results = db.tx(() -> db.indexedTable().query()
                .where("keyId").gte("a")
                .limit(2)
                .index(IndexedEntity.KEY_INDEX)
                .unordered()
                .find());
        assertThat(results).containsExactly(ie2, ie1);
    }

    @Test
    public void multipleTablesSameEntitySameTransaction() {
        UniqueProject ue = new UniqueProject(new UniqueProject.Id("id1"), "valuableName", 1);
        db.tx(() -> db.table(UniqueProject.class).save(ue));

        List<UniqueProject> findFirstTableThenSecond = db.tx(() -> {
            var p1 = db.table(UniqueProject.class).find(ue.getId());
            var p2 = db.table(TestEntities.SECOND_UNIQUE_PROJECT_TABLE).find(ue.getId());
            return Stream.of(p1, p2).filter(Objects::nonNull).toList();
        });

        List<UniqueProject> findSecondTableThenFirst = db.tx(() -> {
            var p1 = db.table(TestEntities.SECOND_UNIQUE_PROJECT_TABLE).find(ue.getId());
            var p2 = db.table(UniqueProject.class).find(ue.getId());
            return Stream.of(p1, p2).filter(Objects::nonNull).toList();
        });

        assertThat(findFirstTableThenSecond).isEqualTo(findSecondTableThenFirst);
    }

    @Test
    public void multipleTablesSameEntitySameTransactionQueryView() {
        UniqueProject ue = new UniqueProject(new UniqueProject.Id("id1"), "valuableName", 1);
        db.tx(() -> db.table(UniqueProject.class).save(ue));

        List<UniqueProject.NameView> findFirstTableThenSecond = db.tx(() -> {
            var n1 = db.table(UniqueProject.class).find(UniqueProject.NameView.class, ue.getId());
            var n2 = db.table(TestEntities.SECOND_UNIQUE_PROJECT_TABLE).find(UniqueProject.NameView.class, ue.getId());
            return Stream.of(n1, n2).filter(Objects::nonNull).toList();
        });

        List<UniqueProject.NameView> findSecondTableThenFirst = db.tx(() -> {
            var n1 = db.table(TestEntities.SECOND_UNIQUE_PROJECT_TABLE).find(UniqueProject.NameView.class, ue.getId());
            var n2 = db.table(UniqueProject.class).find(UniqueProject.NameView.class, ue.getId());
            return Stream.of(n1, n2).filter(Objects::nonNull).toList();
        });

        assertThat(findFirstTableThenSecond).isEqualTo(findSecondTableThenFirst);
    }

    @Test
    public void queryStatsCollectionMode() {
        db.tx(() -> {
            for (int i = 0; i < 1_000; i++) {
                var ue = new UniqueProject(new UniqueProject.Id("id" + i), "valuableName-" + i, i);
                db.table(UniqueProject.class).save(ue);
            }
        });

        var found = new StdTxManager(repository)
                .withName("query-stats")
                .withVerboseLogging()
                .withQueryStats(QueryStatsMode.FULL)
                .readOnly()
                .noFirstLevelCache()
                .withStatementIsolationLevel(IsolationLevel.SNAPSHOT)
                .run(() -> db.table(UniqueProject.class).query()
                        .where("id").in(List.of(
                                new UniqueProject.Id("id501"),
                                new UniqueProject.Id("id502"),
                                new UniqueProject.Id("id503"),
                                new UniqueProject.Id("id999")
                        ))
                        .find());
        assertThat(found).hasSize(4);
    }

    @AllArgsConstructor
    private static class DelegateSchemeServiceImplBase extends SchemeServiceGrpc.SchemeServiceImplBase {
        @Delegate
        final SchemeServiceGrpc.SchemeServiceStub schemeServiceStub;
    }

    @AllArgsConstructor
    private static class DelegateTopicServiceImplBase extends TopicServiceGrpc.TopicServiceImplBase {
        @Delegate
        final TopicServiceGrpc.TopicServiceStub topicServiceStub;
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

    private static class ProxyYdbTableService extends TableServiceGrpc.TableServiceImplBase {
        @Delegate(excludes = ProxyYdbTableService.OverriddenMethod.class)
        TableServiceGrpc.TableServiceStub tableServiceStub;

        ProxyYdbTableService(ManagedChannel channel) {
            tableServiceStub = TableServiceGrpc.newStub(channel);
        }

        @Override
        public void executeDataQuery(tech.ydb.proto.table.YdbTable.ExecuteDataQueryRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.ExecuteDataQueryResponse> responseObserver) {
            tableServiceStub.executeDataQuery(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(tech.ydb.proto.table.YdbTable.ExecuteDataQueryResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        @Override
        public void commitTransaction(tech.ydb.proto.table.YdbTable.CommitTransactionRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.CommitTransactionResponse> responseObserver) {
            tableServiceStub.commitTransaction(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(tech.ydb.proto.table.YdbTable.CommitTransactionResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        @Override
        public void rollbackTransaction(tech.ydb.proto.table.YdbTable.RollbackTransactionRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.RollbackTransactionResponse> responseObserver) {
            tableServiceStub.rollbackTransaction(request, new ProxyYdbTableService.DelegateStreamObserver<>(responseObserver) {
                @Override
                public void onNext(tech.ydb.proto.table.YdbTable.RollbackTransactionResponse response) {
                    super.onNext(response.toBuilder().setOperation(breakOperation(response.getOperation())).build());
                }
            });
        }

        private OperationProtos.Operation breakOperation(OperationProtos.Operation operation) {
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
            void executeDataQuery(tech.ydb.proto.table.YdbTable.ExecuteDataQueryRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.ExecuteDataQueryResponse> responseObserver);

            void commitTransaction(tech.ydb.proto.table.YdbTable.CommitTransactionRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.CommitTransactionResponse> responseObserver);

            void rollbackTransaction(tech.ydb.proto.table.YdbTable.RollbackTransactionRequest request, StreamObserver<tech.ydb.proto.table.YdbTable.RollbackTransactionResponse> responseObserver);
        }
    }
}
