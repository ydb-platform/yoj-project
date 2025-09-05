package tech.ydb.yoj.repository.ydb.statement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.junit.ClassRule;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.test.RepositoryTestSupport;
import tech.ydb.yoj.repository.ydb.YdbConfig;
import tech.ydb.yoj.repository.ydb.YdbEnvAndTransportRule;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction;

public abstract class AbstractMultipleVarsYqlStatementIntegrationTestBase extends RepositoryTestSupport {
    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    protected static final TestEntity ENTITY_1 = new TestEntity(TestEntity.Id.of("a"), "foo");
    protected static final TestEntity ENTITY_1_1 = new TestEntity(TestEntity.Id.of("a"), "fuu");
    protected static final TestEntity ENTITY_2 = new TestEntity(TestEntity.Id.of("b"), "bar");
    protected static final TestEntity ENTITY_3 = new TestEntity(TestEntity.Id.of("c"), "ops");
    private static final YdbConfig config =
            YdbConfig.createForTesting(
                    "",
                    0,
                    "/local/ycloud/multiple_vars_yql_statement/",
                    "/local"
            );

    @Override
    protected Repository createRepository() {
        var repository = new YdbRepository(ydbEnvAndTransport.getYdbConfig(), ydbEnvAndTransport.getGrpcTransport()) {
            @Override
            public RepositoryTransaction startTransaction(TxOptions options) {
                return new RepositoryTransactionImpl(this, options);
            }

            static class RepositoryTransactionImpl extends YdbRepositoryTransaction<YdbRepository> implements TestDb {
                RepositoryTransactionImpl(YdbRepository repo, TxOptions options) {
                    super(repo, options);
                }
            }
        };

        repository.getSchemaOperations().createTablespace();
        repository.getSchemaOperations().createTable(TableDescriptor.from(EntitySchema.of(TestEntity.class)));

        return repository;
    }

    protected TestDb getTestDb() {
        return BaseDb.current(TestDb.class);
    }

    protected tech.ydb.yoj.repository.db.Table<TestEntity> getTestEntityTable() {
        return getTestDb().table(TestEntity.class);
    }

    protected TxManager getTxManager() {
        return new StdTxManager(repository);
    }

    public interface TestDb extends BaseDb {
        <PARAMS> void pendingExecute(Statement<PARAMS, ?> statement, PARAMS value);
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Builder(toBuilder = true)
    @Table(name = "test_entities")
    @Value
    public static class TestEntity implements Entity<TestEntity> {
        public static final String ID_FIELD = "id";
        public static final String VALUE_FIELD = "value";

        @Column(name = ID_FIELD)
        @NonNull
        Id id;

        @Column(name = VALUE_FIELD, dbType = DbType.UTF8)
        @NonNull
        String value;

        @Value(staticConstructor = "of")
        public static class Id implements Entity.Id<TestEntity> {
            @Column(dbType = DbType.UTF8)
            String value;
        }
    }
}
