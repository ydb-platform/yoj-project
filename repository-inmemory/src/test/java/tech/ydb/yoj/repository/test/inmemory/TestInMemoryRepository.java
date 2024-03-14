package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.AbstractDelegatingTable;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableQueryBuilder;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.json.JacksonJsonConverter;
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
        public ComplexTable complexes() {
            return new ComplexTableImpl(table(Complex.class));
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
        public SupabubbleTable supabubbles() {
            return new SupabubbleTable(table(Supabubble.class));
        }

        @Override
        public Supabubble2Table supabubbles2() {
            return new Supabubble2InMemoryTable(getMemory(Supabubble2.class));
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
    }

    private static class Supabubble2InMemoryTable extends InMemoryTable<Supabubble2> implements TestEntityOperations.Supabubble2Table {
        public Supabubble2InMemoryTable(DbMemory<Supabubble2> memory) {
            super(memory);
        }
    }

    private static class ComplexTableImpl extends AbstractDelegatingTable<Complex> implements ComplexTable {
        public ComplexTableImpl(Table<Complex> target) {
            super(target);
        }

        @Override
        public TableQueryBuilder<Complex> query() {
            return super.query();
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

        @Override
        public TableQueryBuilder<IndexedEntity> query() {
            return super.query();
        }
    }
}
