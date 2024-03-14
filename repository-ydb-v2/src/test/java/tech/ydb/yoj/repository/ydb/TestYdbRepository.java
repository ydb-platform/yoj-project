package tech.ydb.yoj.repository.ydb;

import io.grpc.ClientInterceptor;
import tech.ydb.auth.AuthProvider;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableQueryBuilder;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.json.JacksonJsonConverter;
import tech.ydb.yoj.repository.db.statement.Changeset;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.BubbleTable;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.ComplexTable;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.IndexedTable;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.NetworkAppliance;
import tech.ydb.yoj.repository.test.sample.model.Primitive;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Referring;
import tech.ydb.yoj.repository.test.sample.model.Supabubble;
import tech.ydb.yoj.repository.test.sample.model.Supabubble2;
import tech.ydb.yoj.repository.test.sample.model.Team;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.UpdateFeedEntry;
import tech.ydb.yoj.repository.test.sample.model.VersionedAliasedEntity;
import tech.ydb.yoj.repository.test.sample.model.VersionedEntity;
import tech.ydb.yoj.repository.ydb.table.YdbTable;

import java.util.List;
import java.util.Set;

import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.where;

public class TestYdbRepository extends YdbRepository {
    static {
        CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
    }

    @Override
    public RepositoryTransaction startTransaction(TxOptions options) {
        return new TestYdbRepositoryTransaction(this, options);
    }

    public TestYdbRepository(YdbConfig config) {
        super(config);
    }

    public TestYdbRepository(YdbConfig config, AuthProvider authProvider, List<ClientInterceptor> interceptors) {
        super(config, authProvider, interceptors);
    }

    static class TestYdbRepositoryTransaction extends YdbRepositoryTransaction<TestYdbRepository> implements TestEntityOperations {
        public TestYdbRepositoryTransaction(TestYdbRepository repo) {
            this(repo, TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE));
        }

        public TestYdbRepositoryTransaction(TestYdbRepository repo, TxOptions options) {
            super(repo, options);
        }

        @Override
        public ProjectTable projects() {
            return new ProjectTable(table(Project.class));
        }

        @Override
        public BubbleTable bubbles() {
            return new BubbleTableImpl(this);
        }

        @Override
        public ComplexTable complexes() {
            return new ComplexTableImpl(this);
        }

        @Override
        public TypeFreakTable typeFreaks() {
            return new TypeFreakTable(table(TypeFreak.class));
        }

        @Override
        public Table<Primitive> primitives() {
            return table(Primitive.class);
        }

        @Override
        public Table<Referring> referrings() {
            return table(Referring.class);
        }

        @Override
        public Table<LogEntry> logEntries() {
            return table(LogEntry.class);
        }

        @Override
        public Table<Team> teams() {
            return table(Team.class);
        }

        @Override
        public Table<EntityWithValidation> entitiesWithValidation() {
            return table(EntityWithValidation.class);
        }

        @Override
        public IndexedTable indexedTable() {
            return new IndexedTableImpl(this);
        }

        @Override
        public SupabubbleTable supabubbles() {
            return new SupabubbleTable(table(Supabubble.class));
        }

        @Override
        public Supabubble2Table supabubbles2() {
            return new YdbSupabubble2Table(this);
        }

        @Override
        public Table<UpdateFeedEntry> updateFeedEntries() {
            return table(UpdateFeedEntry.class);
        }

        @Override
        public Table<NetworkAppliance> networkAppliances() {
            return table(NetworkAppliance.class);
        }

        @Override
        public Table<VersionedEntity> versionedEntities() {
            return table(VersionedEntity.class);
        }

        public Table<VersionedAliasedEntity> versionedAliasedEntities() {
            return table(VersionedAliasedEntity.class);
        }
    }

    private static class YdbSupabubble2Table extends YdbTable<Supabubble2> implements TestEntityOperations.Supabubble2Table {
        protected YdbSupabubble2Table(QueryExecutor executor) {
            super(executor);
        }

        @Override
        public List<Supabubble2> findLessThan(Supabubble2.Id id) {
            return find(where("id").lt(id));
        }
    }

    private static class ComplexTableImpl extends YdbTable<Complex> implements ComplexTable {
        protected ComplexTableImpl(QueryExecutor executor) {
            super(executor);
        }

        @Override
        public TableQueryBuilder<Complex> query() {
            return super.query();
        }
    }

    private static class BubbleTableImpl extends YdbTable<Bubble> implements BubbleTable {
        protected BubbleTableImpl(QueryExecutor executor) {
            super(executor);
        }

        @Override
        public void updateSomeFields(Set<Bubble.Id> ids, String fieldA, String fieldB) {
            this.updateIn(ids, new Changeset().set("fieldA", fieldA).set("fieldB", fieldB));
        }
    }

    private static class IndexedTableImpl extends YdbTable<IndexedEntity> implements IndexedTable {
        protected IndexedTableImpl(QueryExecutor executor) {
            super(executor);
        }

        @Override
        public void updateSomeFields(Set<IndexedEntity.Id> ids, String value, String value2) {
            this.updateIn(ids, new Changeset().set("valueId", value).set("valueId2", value2));
        }

        @Override
        public TableQueryBuilder<IndexedEntity> query() {
            return super.query();
        }
    }
}
