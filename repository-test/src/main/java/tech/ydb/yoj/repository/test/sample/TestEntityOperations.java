package tech.ydb.yoj.repository.test.sample;

import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.AbstractDelegatingTable;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableQueryBuilder;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.BytePkEntity;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.Primitive;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Referring;
import tech.ydb.yoj.repository.test.sample.model.Supabubble;
import tech.ydb.yoj.repository.test.sample.model.Supabubble2;
import tech.ydb.yoj.repository.test.sample.model.Team;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public interface TestEntityOperations extends BaseDb {
    ProjectTable projects();

    BubbleTable bubbles();

    ComplexTable complexes();

    default Table<BytePkEntity> bytePkEntities() {
        return table(BytePkEntity.class);
    }

    TypeFreakTable typeFreaks();

    Table<Primitive> primitives();

    Table<Referring> referrings();

    Table<LogEntry> logEntries();

    Table<Team> teams();

    Table<EntityWithValidation> entitiesWithValidation();

    IndexedTable indexedTable();

    SupabubbleTable supabubbles();

    Supabubble2Table supabubbles2();

    class ProjectTable extends AbstractDelegatingTable<Project> {
        public ProjectTable(Table<Project> target) {
            super(target);
        }

        public List<Project> findNamed() {
            return query()
                    .where("name").isNotNull()
                    .find();
        }

        public List<Project> findTopNamed(int n) {
            return query()
                    .where("name").isNotNull()
                    .limit(n)
                    .find();
        }

        public void updateName(Project.Id id, String newName) {
            modifyIfPresent(id, t -> t.withName(newName));
        }

        public List<Project> findByPredicateWithManyIds(Collection<Project.Id> ids) {
            return query().where("id").in(ids).find();
        }

        public List<Project> findByPredicateWithManyIdValues(Collection<String> ids) {
            return query().where("id").in(ids).find();
        }

        @Override
        public void bulkUpsert(List<Project> input, BulkParams params) {
            getTarget().bulkUpsert(input, params);
        }
    }

    class TypeFreakTable extends AbstractDelegatingTable<TypeFreak> {
        public TypeFreakTable(Table<TypeFreak> target) {
            super(target);
        }

        public List<TypeFreak> findWithEmbeddedAIn(String possibleA, String... otherPossibleAs) {
            List<String> lst = new ArrayList<>(1 + otherPossibleAs.length);
            lst.add(possibleA);
            lst.addAll(asList(otherPossibleAs));
            return findWithEmbeddedAIn(unmodifiableList(lst));
        }

        public List<TypeFreak> findWithEmbeddedAIn(Collection<String> possibleA) {
            return query().where("embedded.a.a").in(possibleA).find();
        }

        public List<TypeFreak> findWithEmbeddedANotIn(Collection<String> possibleA) {
            return query().where("embedded.a.a").notIn(possibleA).find();
        }

        public List<TypeFreak> findWithEmbeddedBNotEqualTo(String prohibitedB) {
            return query().where("embedded.b.b").neq(prohibitedB).find();
        }

        public TypeFreak findByPredicateWithComplexId(TypeFreak.Id id) {
            return find(id);
        }

        public List<TypeFreak.View> findViewWithEmbeddedAIn(Collection<String> possibleA) {
            return query()
                    .where("embedded.a.a").in(possibleA)
                    .find(TypeFreak.View.class);
        }

        public void updateEmbedded(TypeFreak.Id id, TypeFreak.Embedded newEmbedded) {
            modifyIfPresent(id, t -> t.withEmbedded(newEmbedded));
        }
    }

    interface TableWithQueryBuilder<T extends Entity<T>> extends Table<T> {
        @Override
        TableQueryBuilder<T> query();
    }

    interface ComplexTable extends TableWithQueryBuilder<Complex> {
    }

    interface BubbleTable extends Table<Bubble> {
        void updateSomeFields(Set<Bubble.Id> ids, String fieldA, String fieldB);
    }

    interface IndexedTable extends TableWithQueryBuilder<IndexedEntity> {
        void updateSomeFields(Set<IndexedEntity.Id> ids, String value, String value2);
    }

    class SupabubbleTable extends AbstractDelegatingTable<Supabubble> {
        public SupabubbleTable(Table<Supabubble> target) {
            super(target);
        }

        @Override
        public TableQueryBuilder<Supabubble> query() {
            return super.query();
        }
    }

    interface Supabubble2Table extends Table<Supabubble2> {
        default List<Supabubble2> findLessThan(Supabubble2.Id id) {
            throw new UnsupportedOperationException();
        }
    }
}
