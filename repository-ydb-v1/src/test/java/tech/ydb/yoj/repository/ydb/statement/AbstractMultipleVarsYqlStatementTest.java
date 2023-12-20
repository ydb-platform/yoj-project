package tech.ydb.yoj.repository.ydb.statement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.test.RepositoryTestSupport;
import tech.ydb.yoj.repository.ydb.DbType;
import tech.ydb.yoj.repository.ydb.TestYdbConfig;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.YdbRepositoryTransaction;

public abstract class AbstractMultipleVarsYqlStatementTest extends RepositoryTestSupport {

    protected static final TestEntity ENTITY_1 = new TestEntity(TestEntity.Id.of("a"), "foo");
    protected static final TestEntity ENTITY_1_1 = new TestEntity(TestEntity.Id.of("a"), "fuu");
    protected static final TestEntity ENTITY_2 = new TestEntity(TestEntity.Id.of("b"), "bar");
    protected static final TestEntity ENTITY_3 = new TestEntity(TestEntity.Id.of("c"), "ops");

    @Override
    protected Repository createRepository() {
        var repository = new YdbRepository(TestYdbConfig.create(getClass().getSimpleName())) {
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

        repository.createTablespace();
        repository.schema(TestEntity.class).create();

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
