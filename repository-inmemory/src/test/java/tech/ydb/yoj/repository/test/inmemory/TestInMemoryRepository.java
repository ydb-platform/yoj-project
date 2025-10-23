package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.AbstractDelegatingTable;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.json.JacksonJsonConverter;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.BubbleTable;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.IndexedTable;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations.Supabubble2Table;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.DetachedEntity;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity2;
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

import java.util.Set;

public class TestInMemoryRepository extends InMemoryRepository {
    static {
        CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
    }

    @Override
    public RepositoryTransaction startTransaction(TxOptions options) {
        return new TestInMemoryRepositoryTransaction(options, this);
    }

    private static class TestInMemoryRepositoryTransaction extends InMemoryRepositoryTransaction implements TestEntityOperations {
        private TestInMemoryRepositoryTransaction(TxOptions options, InMemoryRepository repository) {
            super(options, repository);
        }

        @Override
        public ProjectTable projects() {
            return new ProjectTable(table(Project.class));
        }

        @Override
        public BubbleTable bubbles() {
            return new BubbleTableImpl(table(Bubble.class));
        }

        @Override
        public Table<Complex> complexes() {
            return table(Complex.class);
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
            return new IndexedTableImpl(table(IndexedEntity.class));
        }

        @Override
        public Table<Supabubble> supabubbles() {
            return table(Supabubble.class);
        }

        @Override
        public Supabubble2Table supabubbles2() {
            return new Supabubble2InMemoryTable(table(Supabubble2.class));
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

        @Override
        public Table<VersionedAliasedEntity> versionedAliasedEntities() {
            return table(VersionedAliasedEntity.class);
        }

        @Override
        public Table<DetachedEntity> detachedEntities() {
            return table(DetachedEntity.class);
        }

        @Override
        public Table<MultiWrappedEntity> multiWrappedIdEntities() {
            return table(MultiWrappedEntity.class);
        }

        @Override
        public Table<MultiWrappedEntity2> multiWrappedEntities2() {
            return table(MultiWrappedEntity2.class);
        }
    }

    private static class Supabubble2InMemoryTable extends AbstractDelegatingTable<Supabubble2> implements Supabubble2Table {
        public Supabubble2InMemoryTable(Table<Supabubble2> target) {
            super(target);
        }
    }

    private static class BubbleTableImpl extends AbstractDelegatingTable<Bubble> implements BubbleTable {
        public BubbleTableImpl(Table<Bubble> target) {
            super(target);
        }

        @Override
        public void updateSomeFields(Set<Bubble.Id> ids, String fieldA, String fieldB) {
            throw new UnsupportedOperationException("not for in-memory");
        }
    }

    private static class IndexedTableImpl extends AbstractDelegatingTable<IndexedEntity> implements IndexedTable {
        protected IndexedTableImpl(Table<IndexedEntity> target) {
            super(target);
        }

        @Override
        public void updateSomeFields(Set<IndexedEntity.Id> ids, String value, String value2) {
            throw new UnsupportedOperationException("not for in-memory");
        }
    }
}
