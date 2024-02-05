package tech.ydb.yoj.repository.test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.SchemaOperations;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.repository.db.exception.DropTableException;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionIsolationLevelException;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionScanException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations;
import tech.ydb.yoj.repository.test.sample.model.Book;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.BytePkEntity;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.Complex.Id;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.MultiLevelDirectory;
import tech.ydb.yoj.repository.test.sample.model.NonDeserializableEntity;
import tech.ydb.yoj.repository.test.sample.model.NonDeserializableObject;
import tech.ydb.yoj.repository.test.sample.model.Primitive;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Referring;
import tech.ydb.yoj.repository.test.sample.model.Simple;
import tech.ydb.yoj.repository.test.sample.model.Supabubble;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak.A;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak.B;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak.Embedded;
import tech.ydb.yoj.repository.test.sample.model.UpdateFeedEntry;
import tech.ydb.yoj.repository.test.sample.model.WithUnflattenableField;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;

@SuppressWarnings("checkstyle:MethodCount")
public abstract class RepositoryTest extends RepositoryTestSupport {
    protected TestDb db;

    @Override
    protected Repository createRepository() {
        return TestEntities.init(createTestRepository());
    }

    @Override
    public void setUp() {
        super.setUp();
        this.db = new TestDbImpl<>(this.repository);
    }

    @Override
    public void tearDown() {
        this.db = null;
        super.tearDown();
    }

    protected abstract Repository createTestRepository();

    @Test
    public void schema() {
        SchemaOperations<Simple> schema = repository.schema(Simple.class);
        schema.create();
        schema.create(); // second create doesn't fail
        schema.drop();
        assertThatExceptionOfType(DropTableException.class).isThrownBy(schema::drop);  // second drop fails
    }

    @Test
    public void multiLevelDirectorySchema() {
        SchemaOperations<MultiLevelDirectory> schema = repository.schema(MultiLevelDirectory.class);
        schema.create();
    }

    @Test
    public void snapshotWithSubfolders() {
        TxManager txManager = new StdTxManager(repository);

        checkEmpty(txManager);

        String initSnapshotId = repository.makeSnapshot();

        txManager.tx(() -> {
            TestEntityOperations db = BaseDb.current(TestEntityOperations.class);

            db.projects().save(new Project(new Project.Id("12312"), "ssss"));
            db.projects().save(new Project(new Project.Id("123123"), "asa"));

            db.table(Primitive.class).save(new Primitive(new Primitive.Id(121), 3));
            db.table(Primitive.class).save(new Primitive(new Primitive.Id(122), 5));
        });

        checkNotEmpty(txManager);

        // make ne snapshot and load initial
        String snapshotWithDataId = repository.makeSnapshot();
        repository.loadSnapshot(initSnapshotId);

        // must be empty after load of initial snapshot
        checkEmpty(txManager);

        // load snapshot created after inserts and check entities present
        repository.loadSnapshot(snapshotWithDataId);
        checkNotEmpty(txManager);
    }

    private void checkNotEmpty(TxManager txManager) {
        txManager.tx(() -> {
            TestEntityOperations db = BaseDb.current(TestEntityOperations.class);

            Assert.assertNotNull(db.projects().find(new Project.Id("12312")));
            Assert.assertNotNull(db.projects().find(new Project.Id("123123")));

            Assert.assertNotNull(db.table(Primitive.class).find(new Primitive.Id(121)));
            Assert.assertNotNull(db.table(Primitive.class).find(new Primitive.Id(122)));
        });
    }

    private void checkEmpty(TxManager txManager) {
        txManager.tx(() -> {
            TestEntityOperations db = BaseDb.current(TestEntityOperations.class);

            Assert.assertEquals(0, db.projects().streamAllIds(10_000).count());
            Assert.assertEquals(0, db.table(Primitive.class).streamAllIds(10_000).count());
        });
    }

    @Test
    public void listWithQueryByNullableField() {
        Project projectWithName1 = db.tx(() -> db.projects().save(new Project(new Project.Id("1"), "named_1")));
        assertEquals("named_1", projectWithName1.getName());
        Project projectWithName2 = db.tx(() -> db.projects().save(new Project(new Project.Id("2"), "named_2")));
        assertEquals("named_2", projectWithName2.getName());
        Project projectWithoutName = db.tx(() -> db.projects().save(new Project(new Project.Id("3"), null)));
        assertNull(projectWithoutName.getName());
        assertEquals(3, db.readOnly().run(() -> db.projects().findAll()).size());

        FilterExpression<Project> eqNotNullExp = newFilterBuilder(Project.class)
                .where("name").eq("named_1")
                .build();
        testList(eqNotNullExp, projectWithName1);

        FilterExpression<Project> neqNotNullExp = newFilterBuilder(Project.class)
                .where("name").neq("named_1")
                .build();
        testList(neqNotNullExp, projectWithName2);

        FilterExpression<Project> notNullExp = newFilterBuilder(Project.class)
                .where("name").isNotNull()
                .build();
        testList(notNullExp, projectWithName1, projectWithName2);

        FilterExpression<Project> nullExp = newFilterBuilder(Project.class)
                .where("name").isNull()
                .build();
        testList(nullExp, projectWithoutName);
    }

    @Test
    public void simpleCrudInDifferentTx() {
        Project p1 = db.tx(() -> {
            Project p = new Project(new Project.Id("1"), "named");
            db.projects().save(p);
            return p;
        });

        Project p2 = db.tx(() -> db.projects().find(new Project.Id("1")));
        assertEquals(p1, p2);

        Project p3 = db.tx(() -> {
            Project p = new Project(new Project.Id("1"), "renamed");
            db.projects().save(p);
            return p;
        });
        Project p4 = db.tx(() -> db.projects().find(new Project.Id("1")));
        assertEquals(p3, p4);

        db.tx(() -> db.projects().delete(new Project.Id("1")));
        Project p5 = db.tx(() -> db.projects().find(new Project.Id("1")));
        assertNull(p5);
    }

    @Test
    public void deferAfterCommitDontRunInDryRun() {
        db.withDryRun(true).tx(
                () -> Tx.Current.get().defer(
                        () -> Assert.fail("defer after commit musn't call in dry run")
                )
        );
    }

    @Test
    public void deferNotInTxContext() {
        db.tx(
                () -> Tx.Current.get().defer(
                        () -> assertFalse(Tx.Current.exists())
                )
        );
    }

    @Test
    public void deferFinallyDontRunInDryRun() {
        db.withDryRun(true).tx(
                () -> Tx.Current.get().deferFinally(
                        () -> Assert.fail("defer after commit musn't call in dry run")
                )
        );
    }

    @Test
    public void deferFinallyCommit() {
        AtomicInteger executions = new AtomicInteger();
        db.tx(() -> Tx.Current.get().deferFinally(executions::incrementAndGet));
        assertEquals(1, executions.get());
    }

    @Test
    public void deferFinallyRollback() {
        AtomicInteger executions = new AtomicInteger();
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> db.tx(() -> {
            Tx.Current.get().deferFinally(executions::incrementAndGet);
            throw new RuntimeException();
        }));
        assertEquals(1, executions.get());
    }

    @Test
    public void deferFinallyRollbackRetryable() {
        AtomicInteger executions = new AtomicInteger();
        assertThatExceptionOfType(UnavailableException.class).isThrownBy(() -> db.tx(() -> {
            Tx.Current.get().deferFinally(executions::incrementAndGet);
            throw new OptimisticLockException("");
        }));
        assertEquals(1, executions.get());
    }

    @Test
    public void deferFinallyNotInTxContext() {
        db.tx(() -> Tx.Current.get().deferFinally(() -> assertFalse(Tx.Current.exists())));
    }

    @Test
    public void deferFinallyRollbackNotInTxContext() {
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> db.tx(() -> {
            Tx.Current.get().deferFinally(() -> assertFalse(Tx.Current.exists()));
            throw new RuntimeException();
        }));
    }

    @Test
    public void findAll() {
        List<Project> all = db.tx(() -> db.projects().findAll());
        assertEquals(0, all.size());

        Project p1 = db.tx(() -> {
            Project p = new Project(new Project.Id("1"), "p1");
            db.projects().save(p);
            return p;
        });
        List<Project> all1 = db.tx(() -> db.projects().findAll());
        assertEquals(1, all1.size());
        assertEquals(p1, all1.get(0));

        db.tx(() -> db.projects().save(new Project(new Project.Id("2"), "p2")));
        List<Project> all2 = db.tx(() -> db.projects().findAll());
        assertEquals(2, all2.size());

        db.tx(() -> db.projects().deleteAll());
        List<Project> all3 = db.tx(() -> db.projects().findAll());
        assertEquals(0, all3.size());
    }

    @Test
    public void streamAll() {
        for (int i = 1; i < 5; i++) {
            Project p = new Project(new Project.Id(String.valueOf(i)), "");
            db.tx(() -> db.projects().save(p));
            assertThat(db.tx(() -> db.projects().streamAll(2).collect(toList())))
                    .hasSize(i);
        }

        assertThat(db.tx(() -> db.projects().streamAll(2).limit(1).collect(toList())))
                .hasSize(1);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().streamAll(0)));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().streamAll(5001)));
    }

    @Test
    public void readTable() {
        assertThat(db.readOnly().run(() -> db.projects().readTable(ReadTableParams.getDefault()).count())).isEqualTo(0);

        List<Project> expectedProjects = new ArrayList<>();
        for (int i = 1; i <= 9; ++i) {
            Project p = new Project(new Project.Id(String.valueOf(i)), "project-" + i);
            expectedProjects.add(p);
            db.tx(() -> db.projects().save(p));
            assertThat(db.readOnly().run(() -> db.projects().readTable(ReadTableParams.getDefault()).count()))
                    .isEqualTo(i);
        }

        ReadTableParams<Project.Id> readFrom = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjects.get(3).getId())
                .rowLimit(5)
                .ordered()
                .build();
        ReadTableParams<Project.Id> readFromUnordered = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjects.get(3).getId())
                .rowLimit(5)
                .build();
        assertThat(
                db.readOnly().run(() -> db.projects().readTable(readFrom)
                        .map(p -> p.getId().getValue())
                        .collect(Collectors.toList()))
        ).isEqualTo(
                expectedProjects.subList(3, 8).stream()
                        .map(p -> p.getId().getValue())
                        .collect(Collectors.toList())
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.readOnly().run(() -> db.projects().readTable(readFromUnordered)));
        ReadTableParams<Project.Id> readFromTo = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjects.get(3).getId())
                .toKey(expectedProjects.get(7).getId())
                .ordered()
                .build();
        ReadTableParams<Project.Id> readFromToUnordered = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjects.get(3).getId())
                .toKey(expectedProjects.get(7).getId())
                .build();
        assertThat(
                db.readOnly().run(() -> db.projects().readTable(readFromTo)
                        .map(p -> p.getId().getValue())
                        .collect(Collectors.toList()))
        ).isEqualTo(
                expectedProjects.subList(3, 7).stream()
                        .map(p -> p.getId().getValue())
                        .collect(Collectors.toList())
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.readOnly().run(() -> db.projects().readTable(readFromToUnordered)));
        assertThatExceptionOfType(IllegalTransactionIsolationLevelException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().readTable(ReadTableParams.getDefault()).count()));
    }

    @Test
    public void readTableIds() {
        assertThat(db.readOnly().run(() -> db.projects().readTableIds(ReadTableParams.getDefault()).count()))
                .isEqualTo(0);

        List<Project.Id> expectedProjectIds = new ArrayList<>();
        for (int i = 1; i <= 9; ++i) {
            Project p = new Project(new Project.Id(String.valueOf(i)), "project-" + i);
            expectedProjectIds.add(p.getId());
            db.tx(() -> db.projects().save(p));
            assertThat(db.readOnly().run(() -> db.projects().readTableIds(ReadTableParams.getDefault()).count()))
                    .isEqualTo(i);
        }

        ReadTableParams<Project.Id> readFrom = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjectIds.get(3))
                .rowLimit(5)
                .ordered()
                .build();
        ReadTableParams<Project.Id> readFromUnordered = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjectIds.get(3))
                .rowLimit(5)
                .build();
        assertThat(
                db.readOnly().run(() -> db.projects().readTableIds(readFrom)
                        .map(Project.Id::getValue)
                        .collect(Collectors.toList()))
        ).isEqualTo(
                expectedProjectIds.subList(3, 8).stream()
                        .map(Project.Id::getValue)
                        .collect(Collectors.toList())
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.readOnly().run(() -> db.projects().readTableIds(readFromUnordered)));
        ReadTableParams<Project.Id> readFromTo = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjectIds.get(3))
                .toKey(expectedProjectIds.get(7))
                .ordered()
                .build();
        ReadTableParams<Project.Id> readFromToUnordered = ReadTableParams.<Project.Id>builder()
                .fromKeyInclusive(expectedProjectIds.get(3))
                .toKey(expectedProjectIds.get(7))
                .build();
        assertThat(
                db.readOnly().run(() -> db.projects().readTableIds(readFromTo)
                        .map(Project.Id::getValue)
                        .collect(Collectors.toList()))
        ).isEqualTo(
                expectedProjectIds.subList(3, 7).stream()
                        .map(Project.Id::getValue)
                        .collect(Collectors.toList())
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.readOnly().run(() -> db.projects().readTableIds(readFromToUnordered)));
        assertThatExceptionOfType(IllegalTransactionIsolationLevelException.class)
                .isThrownBy(() -> db.tx(() -> db.projects().readTableIds(ReadTableParams.getDefault()).count()));
    }

    @Test
    public void readTableViews() {
        assertThat(db.readOnly().run(() -> db.typeFreaks().readTableIds(ReadTableParams.getDefault()).count()))
                .isEqualTo(0);

        List<TypeFreak.View> expectedViews = new ArrayList<>();
        int savedCount = 0;
        for (int i = 0; i < 100; i++) {
            TypeFreak tf = newTypeFreak(i, "AAA" + (i + 1), "bbb");

            db.tx(() -> db.typeFreaks().save(tf));
            savedCount++;

            if (i < 50) {
                expectedViews.add(new TypeFreak.View(tf.getId(), tf.getEmbedded()));
                assertThat(db.readOnly().run(() -> db.typeFreaks()
                        .readTable(TypeFreak.View.class, ReadTableParams.getDefault()).count())).isEqualTo(savedCount);
            }
        }

        ReadTableParams<TypeFreak.Id> readFrom = ReadTableParams.<TypeFreak.Id>builder()
                .fromKeyInclusive(expectedViews.get(0).getId())
                .rowLimit(expectedViews.size())
                .ordered()
                .build();
        assertThat(db.readOnly().run(() -> db.typeFreaks().readTable(TypeFreak.View.class, readFrom).collect(toList())))
                .isEqualTo(expectedViews);

        ReadTableParams<TypeFreak.Id> readFromTo = ReadTableParams.<TypeFreak.Id>builder()
                .fromKeyInclusive(expectedViews.get(0).getId())
                .toKeyInclusive(expectedViews.get(expectedViews.size() - 1).getId())
                .ordered()
                .build();
        assertThat(db.readOnly().run(() -> db.typeFreaks().readTable(TypeFreak.View.class, readFromTo).collect(toList())))
                .isEqualTo(expectedViews);

        assertThatExceptionOfType(IllegalTransactionIsolationLevelException.class)
                .isThrownBy(() -> db.tx(() -> db.typeFreaks().readTableIds(ReadTableParams.getDefault()).count()));
    }

    @Test
    public void doNotCommitAfterTLI() {
        Project.Id id1 = new Project.Id("id1");
        Project.Id id2 = new Project.Id("id2");

        RepositoryTransaction tx = repository.startTransaction(
                TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)
                        .withImmediateWrites(true)
                        .withFirstLevelCache(false)
        );

        tx.table(Project.class).find(id2);

        db.tx(() -> db.projects().save(new Project(id2, "name2")));

        tx.table(Project.class).save(new Project(id1, "name1")); // make tx available for TLI

        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(() -> tx.table(Project.class).find(id2));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(tx::commit);

        tx.rollback(); // YOJ-tx rollback is possible. session.rollbackCommit() won't execute
    }

    @Test
    public void writeDontProduceTLI() {
        Project.Id id = new Project.Id("id");

        db.tx(() -> db.projects().save(new Project(id, "name")));

        RepositoryTransaction tx = repository.startTransaction(
                TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)
                        .withImmediateWrites(true)
                        .withFirstLevelCache(false)
        );

        tx.table(Project.class).find(id);

        db.tx(() -> {
            db.projects().find(id);
            db.projects().save(new Project(id, "name2"));
        });

        // write don't produce TLI
        tx.table(Project.class).save(new Project(id, "name3"));

        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(tx::commit);
    }

    @Test
    public void consistencyCheckAllColumnsOnFind() {
        Project.Id id1 = new Project.Id("id1");
        Project.Id id2 = new Project.Id("id2");

        db.tx(() -> {
            db.projects().save(new Project(id1, "name"));
            db.projects().save(new Project(id2, "name"));
        });

        RepositoryTransaction tx = repository.startTransaction(
                TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE)
                        .withImmediateWrites(true)
                        .withFirstLevelCache(false)
        );

        tx.table(Project.class).save(new Project(new Project.Id("id3"), "name")); // make tx available for TLI

        tx.table(Project.class).find(id1);
        tx.table(Project.class).find(id2);

        db.tx(() -> {
            db.projects().find(id2);
            db.projects().save(new Project(id2, "name2"));
        });

        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(() -> tx.table(Project.class).find(id1));
    }

    @Test
    public void streamAllWithPartitioning() {
        db.tx(() -> {
            db.complexes().insert(new Complex(new Complex.Id(0, 0L, "0", Complex.Status.OK)));
        });

        assertThat(db.tx(() -> {
            ArrayList<Object> found = new ArrayList<>();
            Iterators.partition(
                            db.complexes()
                                    .streamAll(100)
                                    .map(Complex::getId)
                                    .iterator(), 100)
                    .forEachRemaining(found::addAll);
            return found;
        })).hasSize(1);
    }

    @Test
    public void viewStreamAll() {
        for (int i = 1; i < 5; i++) {
            Book b = new Book(new Book.Id(String.valueOf(i)), i, "title-" + i, emptyList());
            db.tx(() -> db.table(Book.class).save(b));
            assertThat(db.tx(() -> db.table(Book.class).streamAll(Book.TitleViewId.class, 2).collect(toList())))
                    .hasSize(i);
        }
        db.tx(() -> db.table(Book.class)
                .streamAll(Book.TitleViewId.class, 100)
                .forEach(titleView -> assertThat(titleView.getTitle()).isNotBlank()));

        assertThat(db.tx(() -> db.table(Book.class).streamAll(Book.TitleViewId.class, 2)
                .limit(1).collect(toList())))
                .hasSize(1);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.tx(() -> db.table(Book.class).streamAll(Book.TitleViewId.class, 0)));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> db.tx(() -> db.table(Book.class).streamAll(Book.TitleViewId.class, 5001)));
    }

    @Test
    public void streamAllComposite() {
        db.tx(this::makeComplexes);
        assertThat(
                db.tx(() -> db.complexes().streamAll(2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().findAll())
        );
    }

    @Test
    public void streamEmpty() {
        db.tx(() -> assertThat(db.complexes().streamAll(2)).isEmpty());
    }

    @Test
    public void streamPartial() {
        db.tx(this::makeComplexes);
        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(0, 0L, "aaa", Complex.Status.OK), 2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", Complex.Status.OK))))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(0, 0L, "aaa", null), 2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", null))))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(0, 0L, null, null), 2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, null, null))))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(0, null, null, null), 2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, null, null, null))))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(null, null, null, null), 2).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(null, null, null, null))))
        );
    }

    @Test
    public void streamPartialWithPartitioning() {
        db.tx(() -> {
            db.complexes().insert(new Complex(new Complex.Id(0, 0L, "0", Complex.Status.OK)));
        });

        assertThat(
                db.tx(() -> db.complexes().streamPartial(new Complex.Id(0, null, null, null), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(null, null, null, null))))
        );


        assertThat(db.tx(() -> {
            ArrayList<Object> found = new ArrayList<>();
            Iterators.partition(
                            db.complexes()
                                    .streamPartial(new Complex.Id(0, null, null, null), 100)
                                    .map(Complex::getId)
                                    .iterator(), 100)
                    .forEachRemaining(found::addAll);
            return found;
        })).hasSize(1);
    }

    @Test
    public void streamPartialIds() {
        db.tx(this::makeComplexes);
        assertThat(
                db.tx(() -> db.complexes().streamPartialIds(new Complex.Id(0, 0L, "aaa", Complex.Status.OK), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", Complex.Status.OK)))
                        .stream().map(Complex::getId).collect(toList()))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartialIds(new Complex.Id(0, 0L, "aaa", null), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", null)))
                        .stream().map(Complex::getId).collect(toList()))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartialIds(new Complex.Id(0, 0L, null, null), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, 0L, null, null)))
                        .stream().map(Complex::getId).collect(toList()))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartialIds(new Complex.Id(0, null, null, null), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(0, null, null, null)))
                        .stream().map(Complex::getId).collect(toList()))
        );
        assertThat(
                db.tx(() -> db.complexes().streamPartialIds(new Complex.Id(null, null, null, null), 100).collect(toList()))
        ).isEqualTo(
                db.tx(() -> db.complexes().find(new Range<>(new Complex.Id(null, null, null, null)))
                        .stream().map(Complex::getId).collect(toList()))
        );
    }

    @Test
    public void countAll() {
        long allSize = db.tx(() -> db.projects().countAll());
        assertEquals(0, allSize);

        db.tx(() -> db.projects().save(new Project(new Project.Id("1"), "p1")));

        long all1 = db.tx(() -> db.projects().countAll());
        assertEquals(1, all1);

        db.tx(() -> db.projects().save(new Project(new Project.Id("2"), "p2")));
        long all2 = db.tx(() -> db.projects().countAll());
        assertEquals(2, all2);

        db.tx(() -> db.projects().deleteAll());
        long all3 = db.tx(() -> db.projects().countAll());
        assertEquals(0, all3);
    }

    @Test
    public void streamAllIterator() {
        List<Project> projects = IntStream.range(100, 200)
                .mapToObj(i -> new Project(new Project.Id("proj" + i), null))
                .collect(toList());
        db.tx(() -> db.projects().insertAll(projects));

        db.tx(() -> {
            List<Project> projectsViaStreamAllIterator = new ArrayList<>();
            Iterator<Project> iterator = db.projects().streamAll(1_000).iterator();
            while (iterator.hasNext()) {
                projectsViaStreamAllIterator.add(iterator.next());
            }
            assertThat(projectsViaStreamAllIterator).isEqualTo(projects);
        });
    }

    @Test
    public void streamAllMultiPageIterator() {
        List<Project> projects = IntStream.range(200, 300)
                .mapToObj(i -> new Project(new Project.Id("p" + i), null))
                .collect(toList());
        db.tx(() -> db.projects().insertAll(projects));

        db.tx(() -> {
            List<Project> projectsViaStreamAllIterator = new ArrayList<>();
            Iterator<Project> iterator = db.projects().streamAll(13).iterator();
            while (iterator.hasNext()) {
                projectsViaStreamAllIterator.add(iterator.next());
            }
            assertThat(projectsViaStreamAllIterator).isEqualTo(projects);
        });
    }

    @Test
    public void findRange() {
        db.tx(this::makeComplexes);
        db.tx(() -> {
            assertEquals(1, db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", Complex.Status.OK))).size());
            assertEquals(2, db.complexes().find(new Range<>(new Complex.Id(0, 0L, "aaa", null))).size());
            assertEquals(6, db.complexes().find(new Range<>(new Complex.Id(0, 0L, null, null))).size());
            assertEquals(18, db.complexes().find(new Range<>(new Complex.Id(0, null, null, null))).size());
            assertEquals(54, db.complexes().find(new Range<>(new Complex.Id(null, null, null, null))).size());

            assertEquals(4, db.complexes().find(new Range<>(
                    new Complex.Id(0, 0L, "aaa", null),
                    new Complex.Id(0, 0L, "aab", null)
            )).size());
            assertEquals(4, db.complexes().find(new Range<>(
                    new Complex.Id(0, 0L, null, null),
                    new Complex.Id(0, 0L, "aab", null)
            )).size());
            assertEquals(2, db.complexes().find(new Range<>(
                    new Complex.Id(0, 0L, "bbb", null),
                    new Complex.Id(0, 0L, null, null)
            )).size());
            assertEquals(36, db.complexes().find(new Range<>(
                    new Complex.Id(1, null, null, null),
                    new Complex.Id(2, null, null, null)
            )).size());
        });
    }

    @Test
    public void findPartialKeyParallelTransactions() {
        Complex.Id partialId = new Complex.Id(100, 200L, "foo", null);

        RepositoryTransaction tx1 = startTransaction();
        RepositoryTransaction tx2 = startTransaction();
        tx1.table(Complex.class).find(Set.of(partialId));
        tx2.table(Complex.class).find(Set.of(partialId));
        tx1.table(Complex.class).save(new Complex(partialId.withD(Complex.Status.OK)));
        tx2.table(Complex.class).save(new Complex(partialId.withD(Complex.Status.FAIL)));
        tx1.commit();
        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(tx2::commit);
    }

    private RepositoryTransaction startTransaction() {
        return repository.startTransaction(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE));
    }

    protected void makeComplexes() {
        for (int a = 0; a < 3; a++) {
            for (long b = 0; b < 3; b++) {
                for (String c : asList("aaa", "aab", "bbb")) {
                    for (Complex.Status d : Complex.Status.values()) {
                        db.complexes().insert(new Complex(new Complex.Id(a, b, c, d)));
                    }
                }
            }
        }
    }

    @Test
    public void findInCompleteIdAllFromCache() {
        db.tx(this::makeComplexes);
        db.tx(() -> {
            var idsToFetch = ImmutableSet.of(
                    new Id(0, 0L, "aaa", Complex.Status.OK),
                    new Id(0, 0L, "aaa", Complex.Status.FAIL)
            );
            var firstQueryResult = db.complexes().find(idsToFetch).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );
            assertEquals(2, firstQueryResult.size());

            var complexesSecondQuery = db.complexes().find(idsToFetch).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );
            assertEquals(2, complexesSecondQuery.size());

            for (var pair : firstQueryResult.entrySet()) {
                assertSame(
                        pair.getValue(),
                        complexesSecondQuery.get(pair.getKey())
                );
            }
        });
    }

    @Test
    public void findInCompleteIds() {
        /*
        Test that find(Set<>) with complete Ids:
            * checks in cache first (so newly inserted and updated values will be actual)
            * if some ids not found in cache — fetches rest from db
            * returns union of entries collected from cache and db
            * (we cannot test cache empty values here, so somewhere else) correctly populates cache with new values
         */
        var idInDbOnly = new Id(0, 0L, "aaa", Complex.Status.OK);
        var idInDbAndCache = new Id(0, 1L, "aaa", Complex.Status.OK);
        var idInCacheOnly = new Id(0, 3L, "aaa", Complex.Status.OK);
        var idUnexistent = new Id(0, -1L, "aaa", Complex.Status.OK);
        var idUnexistentMarkedEmptyInCache = new Id(0, -2L, "aaa", Complex.Status.OK);

        db.tx(this::makeComplexes);
        db.tx(() -> {
            var entryInDbAndCache = db.complexes().find(idInDbAndCache);
            assertNotNull(entryInDbAndCache);
            assertNull(db.complexes().find(idUnexistentMarkedEmptyInCache));
            var entryInCache = new Complex(idInCacheOnly);
            db.complexes().insert(entryInCache);

            var result = db.complexes().find(ImmutableSet.of(
                    idInDbOnly,
                    idInDbAndCache,
                    idInCacheOnly,
                    idUnexistent,
                    idUnexistentMarkedEmptyInCache
            )).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );

            assertEquals(3, result.size());
            // is same because it is from cache
            assertSame(entryInDbAndCache, result.get(idInDbAndCache));
            // is same because it is from cache (not inserted to db for now)
            assertSame(entryInCache, result.get(idInCacheOnly));
            // this fetched from db and should exist
            assertNotNull(result.get(idInDbOnly));
        });

    }

    @Test
    public void findInPartialIds() {
        /*
        Test that find(Set<>) with partial Ids:
            * queries db
            * checks resulted entries existence in cache by complete id (so entries modified in same tx would be actual)
            * returns union of non-empty cached matched from result and non-matched from db
            * correctly updates cache with values found in db
            * (NOT IMPLEMENTED) also newly created should be searched in cache and fetched
         */
        var idInDbOnly = new Id(0, 0L, "aaa", Complex.Status.OK);
        var idInDbAndUpdatedInCache = new Id(0, 1L, "aaa", Complex.Status.OK);
        // newly created (only in cache) NOT IMPLEMENTED yet
        // var idInCacheOnly = new Id(0, 3L, "aaa", Complex.Status.OK);
        var idUnexistentMarkedEmptyInCache = new Id(0, -2L, "aaa", Complex.Status.OK);
        var createdEntries = new HashMap<Id, Complex>();

        db.tx(() -> {
            var entryOne = db.complexes().insert(new Complex(idInDbOnly));
            var entryTwo = db.complexes().insert(new Complex(idInDbAndUpdatedInCache));
            createdEntries.put(entryOne.getId(), entryOne);
            createdEntries.put(entryTwo.getId(), entryTwo);
        });
        db.tx(() -> {
            var updatedEntry = new Complex(idInDbAndUpdatedInCache, "UPDATED");
            db.complexes().save(updatedEntry);
            assertNull(db.complexes().find(idUnexistentMarkedEmptyInCache));

            var result = db.complexes().find(ImmutableSet.of(
                    new Id(0, null, null, null)
            )).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );

            assertEquals(2, result.size());
            // is same because it is from cache
            assertSame(updatedEntry, result.get(idInDbAndUpdatedInCache));

            // fetched from db, not cache
            var entryIdInDbOnly = result.get(idInDbOnly);
            assertEquals(createdEntries.get(idInDbOnly), entryIdInDbOnly);
            assertNotSame(createdEntries.get(idInDbOnly), entryIdInDbOnly);
            // but in cache now (why not?)
            assertSame(entryIdInDbOnly, db.complexes().find(idInDbOnly));
        });
    }

    @Test
    public void findInCorrectSort() {
        var idA = new Id(0, 0L, "aaa", Complex.Status.OK);
        var idB = new Id(0, 0L, "bbb", Complex.Status.OK);
        var idC = new Id(0, 0L, "ccc", Complex.Status.OK);

        db.tx(() -> {
            db.complexes().insert(new Complex(idA, "A"));
            db.complexes().insert(new Complex(idB, "B"));
            db.complexes().insert(new Complex(idC, "C"));
        });
        db.tx(() -> {
            assertEquals(
                    db.complexes().find(ImmutableSet.of(idA, idB, idC)).stream().map(Entity::getId).collect(toList()),
                    List.of(idA, idB, idC)
            );

            // put in cache
            db.complexes().find(idB);

            assertEquals(
                    db.complexes().find(ImmutableSet.of(idA, idB, idC)).stream().map(Entity::getId).collect(toList()),
                    List.of(idA, idB, idC)
            );
        });
    }

    @Test
    public void findInConsiderDeleted() {
        var firstId = new Id(0, 0L, "aaa", Complex.Status.OK);
        var secondId = new Id(0, 1L, "aaa", Complex.Status.OK);

        db.tx(() -> {
            db.complexes().insert(new Complex(firstId));
            db.complexes().insert(new Complex(secondId));
        });
        db.tx(() -> {
            var result = db.complexes().find(ImmutableSet.of(
                    firstId,
                    secondId
            )).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );

            assertEquals(2, result.size());

            db.complexes().delete(firstId);

            result = db.complexes().find(ImmutableSet.of(
                    firstId,
                    secondId
            )).stream().collect(
                    Collectors.toMap(Complex::getId, Function.identity())
            );

            assertEquals(1, result.size());
        });
    }

    @Test
    public void findInParallelTx() {
        var id = new Id(0, 0L, "aaa", Complex.Status.OK);

        RepositoryTransaction tx0 = startTransaction();
        tx0.table(Complex.class).save(new Complex(id));
        tx0.commit();

        RepositoryTransaction tx1 = startTransaction();
        tx1.table(Complex.class).find(id);
        tx1.table(Complex.class).save(new Complex(id, "updated"));

        RepositoryTransaction tx2 = startTransaction();
        tx2.table(Complex.class).delete(id);

        var found = tx1.table(Complex.class).find(ImmutableSet.of(
                new Id(0, null, null, null)
        ));

        tx2.commit();
        assertNotNull(found);
        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(tx1::commit);
    }

    private int getComplexIdA(int index) {
        return index / 2;
    }

    private Complex.Id getComplexId(int index) {
        var a = getComplexIdA(index);
        var status = index % 2 == 0 ? Complex.Status.OK : Complex.Status.FAIL;

        return new Complex.Id(a, (long) index, String.valueOf(index), status);
    }

    private String getComplexValue(int index) {
        var id = getComplexId(index);

        return "%s-%s-%s-%s".formatted(id.getD(), id.getC(), id.getB(), id.getA());
    }

    private Complex getComplex(int index) {
        return new Complex(getComplexId(index), getComplexValue(index));
    }

    private void fillComplexTableForFindIn() {
        db.tx(() -> IntStream.range(0, 4).mapToObj(this::getComplex).forEach(db.complexes()::save));
    }

    @Test
    public void findInIdsFilteredAndOrdered() {
        var ids = IntStream.range(0, 6).mapToObj(this::getComplexId).collect(toSet());

        findInIdsFilteredAndOrdered(ids);
    }

    @Test
    public void findInPrefixedIdsFilteredAndOrdered() {
        var ids = IntStream.range(0, 6)
                .mapToObj(i -> new Complex.Id(getComplexIdA(i), null, null, null))
                .collect(toSet());

        findInIdsFilteredAndOrdered(ids);
    }

    private void findInIdsFilteredAndOrdered(Set<Complex.Id> ids) {
        fillComplexTableForFindIn();

        var actual = db.tx(() -> db.complexes().query()
                .ids(ids)
                .filter(fb -> fb.where("id.d").eq(Complex.Status.OK))
                .orderBy(ob -> ob.orderBy("value").descending())
                .find()
        );

        assertThat(actual).containsExactly(getComplex(2), getComplex(0));
    }

    @Test
    public void findInIdsViewFilteredAndOrdered() {
        var ids = IntStream.range(0, 6).mapToObj(this::getComplexId).collect(toSet());

        findInIdsViewFilteredAndOrdered(ids);
    }

    @Test
    public void findInPrefixedIdsViewFilteredAndOrdered() {
        var ids = IntStream.of(1, 3, 5)
                .mapToObj(i -> new Complex.Id(getComplexIdA(i), null, null, null))
                .collect(toSet());

        findInIdsViewFilteredAndOrdered(ids);
    }

    public void findInIdsViewFilteredAndOrdered(Set<Complex.Id> ids) {
        fillComplexTableForFindIn();

        var actual = db.tx(() -> db.complexes().query()
                .ids(ids)
                .filter(fb -> fb.where("id.d").eq(Complex.Status.OK))
                .orderBy(ob -> ob.orderBy("value").descending())
                .find(Complex.View.class)
        );

        assertThat(actual).containsExactly(
                new Complex.View(getComplexValue(2)),
                new Complex.View(getComplexValue(0))
        );
    }

    private String getIndexedEntityId(int index) {
        return "id-" + index;
    }

    private String getIndexedEntityKey(int index) {
        return "key-" + index;
    }

    private String getIndexedEntityValue(int index) {
        return "v-" + (index / 3);
    }

    private String getIndexedEntityValue2(int index) {
        return "v2-" + (index % 3);
    }

    private IndexedEntity getIndexedEntity(int index) {
        return new IndexedEntity(
                new IndexedEntity.Id(getIndexedEntityId(index)),
                getIndexedEntityKey(index),
                getIndexedEntityValue(index),
                getIndexedEntityValue2(index)
        );
    }

    private void fillIndexedTableForFindIn() {
        db.tx(() -> IntStream.range(0, 8).mapToObj(this::getIndexedEntity).forEach(db.indexedTable()::save));
    }

    @Test
    public void findInKeysFilteredAndOrdered() {
        findInKeysFilteredAndOrdered(false);
    }

    @Test
    public void findInKeysFilteredAndOrderedLimited() {
        findInKeysFilteredAndOrdered(true);
    }

    @Test
    public void findInPrefixedKeysFilteredAndOrdered() {
        findInPrefixedKeysFilteredAndOrdered(false);
    }

    @Test
    public void findInPrefixedKeysFilteredAndOrderedLimited() {
        findInPrefixedKeysFilteredAndOrdered(true);
    }

    public void findInKeysFilteredAndOrdered(boolean limited) {
        var keys = IntStream.range(0, 10)
                .mapToObj(i -> new IndexedEntity.Key(getIndexedEntityValue(i), getIndexedEntityValue2(i)))
                .collect(toSet());

        findInKeysFilteredAndOrdered(keys, limited);
    }

    private void findInPrefixedKeysFilteredAndOrdered(boolean limited) {
        var keys = IntStream.range(0, 10)
                .mapToObj(i -> new IndexedEntity.Key(getIndexedEntityValue(i), null))
                .collect(toSet());

        findInKeysFilteredAndOrdered(keys, limited);
    }

    private void findInKeysFilteredAndOrdered(Set<IndexedEntity.Key> keys, boolean limited) {
        fillIndexedTableForFindIn();

        var actual = db.tx(() -> {
            var queryBuilder = db.indexedTable().query()
                    .index(IndexedEntity.VALUE_INDEX)
                    .keys(keys)
                    .where("valueId2").neq(getIndexedEntityValue2(1))
                    .orderBy(ob -> ob
                            .orderBy("valueId2").descending()
                            .orderBy("valueId").ascending()
                    );
            if (limited) {
                queryBuilder = queryBuilder.limit(3);
            }

            return queryBuilder.find();
        });

        assertThat(actual).containsExactlyElementsOf(
                IntStream.of(2 /*3*0+2*/, 5 /*3*1+2*/, 0 /*3*0+0*/, 3 /*3*1+0*/, 6 /*3*2+0*/)
                        .limit(limited ? 3 : 10)
                        .mapToObj(this::getIndexedEntity)
                        .toList()
        );
    }

    @Test
    public void findInKeysViewFilteredAndOrdered() {
        findInKeysViewFilteredAndOrdered(false);
    }

    @Test
    public void findInKeysViewFilteredAndOrderedLimited() {
        findInKeysViewFilteredAndOrdered(true);
    }

    @Test
    public void findInPrefixedKeysViewFilteredAndOrdered() {
        findInPrefixedKeysViewFilteredAndOrdered(false);
    }

    @Test
    public void findInPrefixedKeysViewFilteredAndOrderedLimited() {
        findInPrefixedKeysViewFilteredAndOrdered(true);
    }

    @Test
    public void findViewDistinct() {
        fillIndexedTableForFindIn();

        var actualViews = db.tx(() -> db.indexedTable().query().find(IndexedEntity.ValueIdView.class, true));

        var actualValueIds = actualViews.stream()
                .map(IndexedEntity.ValueIdView::getValueId)
                .collect(toSet());

        assertEquals(Set.of("v-0", "v-1", "v-2"), actualValueIds);
    }

    private void findInKeysViewFilteredAndOrdered(boolean limited) {
        var keys = IntStream.range(0, 10)
                .mapToObj(i -> new IndexedEntity.Key(getIndexedEntityValue(i), getIndexedEntityValue2(i)))
                .collect(toSet());

        findInKeysViewFilteredAndOrdered(keys, limited);
    }

    private void findInPrefixedKeysViewFilteredAndOrdered(boolean limited) {
        var keys = IntStream.range(0, 10)
                .mapToObj(i -> new IndexedEntity.Key(getIndexedEntityValue(i), null))
                .collect(toSet());

        findInKeysViewFilteredAndOrdered(keys, limited);
    }

    private void findInKeysViewFilteredAndOrdered(Set<IndexedEntity.Key> keys, boolean limited) {
        fillIndexedTableForFindIn();

        var actual = db.tx(() -> {
            var queryBuilder = db.indexedTable().query()
                    .index(IndexedEntity.VALUE_INDEX)
                    .keys(keys)
                    .where("valueId2").neq(getIndexedEntityValue2(2))
                    .orderBy(ob -> ob.orderBy("id.versionId").descending());
            if (limited) {
                queryBuilder = queryBuilder.limit(3);
            }

            return queryBuilder.find(IndexedEntity.View.class);
        });

        assertThat(actual).containsExactlyElementsOf(
                IntStream.of(7 /*3*2+1*/, 6 /*3*2+0*/, 4 /*3*1+1*/, 3 /*3*1+0*/, 1 /*3*0+1*/, 0 /*3*0+0*/)
                        .limit(limited ? 3 : 10)
                        .mapToObj(i -> new IndexedEntity.View(getIndexedEntityId(i)))
                        .toList()
        );
    }

    @Test
    public void doubleTxIsOk() {
        db.tx(this::findRange);
    }

    @Test(expected = IllegalArgumentException.class)
    public void findPartialId() {
        db.tx(() -> db.bubbles().find(new Bubble.Id("b", null)));
    }

    @Test
    public void fixSnapshotVersionOnFirstQuery() {
        var projectId = new Project.Id("value");

        db.tx(() -> db.tx(() -> db.projects().insert(new Project(projectId, "oldName"))));

        String newName = "name";
        Project project = db.tx(() -> {
            db.separate().tx(() -> db.projects().save(new Project(projectId, newName)));
            return db.projects().find(projectId);
        });
        assertThat(project.getName()).isEqualTo(newName);
    }

    @Test
    public void dontCommitOnUserError() {
        var projectId = new Project.Id("value");
        try {
            db.delayedWrites().tx(() -> {
                db.projects().save(new Project(projectId, "name"));
                throw new RuntimeException("");
            });
        } catch (RuntimeException ignore) {
        }
        assertThat(db.tx(() -> db.projects().find(projectId))).isNull();

        try {
            db.immediateWrites().tx(() -> {
                db.projects().save(new Project(projectId, "name"));
                throw new RuntimeException("");
            });
        } catch (RuntimeException ignore) {
        }
        assertThat(db.tx(() -> db.projects().find(projectId))).isNull();
    }

    @Test
    public void immediateWrites() {
        db.delayedWrites().noFirstLevelCache().tx(() -> {
            var projectId = new Project.Id("value1");
            assertThat(db.projects().find(projectId)).isNull();
            db.projects().save(new Project(projectId, "name"));
            assertThat(db.projects().find(projectId)).isNull();
        });

        db.immediateWrites().noFirstLevelCache().tx(() -> {
            var projectId = new Project.Id("value2");
            var name = "name";
            assertThat(db.projects().find(projectId)).isNull();
            db.projects().save(new Project(projectId, name));
            assertThat(db.projects().find(projectId)).isEqualTo(new Project(projectId, name));
        });
    }

    @Test
    public void snapshotReadWithoutTli() {
        var projectId = new Project.Id("value");

        String newName = "name";
        db.tx(() -> db.tx(() -> db.projects().insert(new Project(projectId, newName))));

        Project project = db.tx(() -> {
            Project findedProject = db.projects().find(projectId);
            db.separate().tx(() -> db.projects().save(new Project(projectId, "invisible")));
            return findedProject;
        });
        assertThat(project.getName()).isEqualTo(newName);
        assertThat(db.tx(() -> db.projects().find(projectId)).getName()).isEqualTo("invisible");

        project = db.tx(() -> {
            db.separate().tx(() -> db.projects().save(new Project(projectId, "invisible")));
            db.projects().find(projectId);
            return db.projects().save(new Project(projectId, newName));
        });
        assertThat(project.getName()).isEqualTo(newName);
    }

    @Test
    public void findByPredicate() {
        db.tx(() -> {
            db.projects().insert(
                    new Project(new Project.Id("unnamed-p1"), null),
                    new Project(new Project.Id("named-p2"), "P2"),
                    new Project(new Project.Id("named-p3"), "P3")
            );

            db.typeFreaks().insert(
                    newTypeFreak(0, "AAA1", "bbb"),
                    newTypeFreak(1, "AAA2", "bbb"),
                    newTypeFreak(2, "AAA3", "bbb"),
                    newTypeFreak(3, "AAA4", "bbb"),

                    newTypeFreak(4, "AAA5", "ccc"),
                    newTypeFreak(5, "AAA6", "ccc"),
                    newTypeFreak(6, "AAA7", "ccc"),
                    newTypeFreak(7, "AAA8", "ccc")
            );
        });

        db.tx(() -> {
            assertThat(db.projects().findNamed()).containsExactlyInAnyOrder(
                    new Project(new Project.Id("named-p3"), "P3"),
                    new Project(new Project.Id("named-p2"), "P2")
            );

            assertThat(db.typeFreaks().findWithEmbeddedAIn("AAA1", "AAA3", "AAA7", "AAA8")).containsExactlyInAnyOrder(
                    newTypeFreak(0, "AAA1", "bbb"),
                    newTypeFreak(2, "AAA3", "bbb"),
                    newTypeFreak(6, "AAA7", "ccc"),
                    newTypeFreak(7, "AAA8", "ccc")
            );

            assertThat(db.typeFreaks().findWithEmbeddedBNotEqualTo("bbb")).containsExactlyInAnyOrder(
                    newTypeFreak(4, "AAA5", "ccc"),
                    newTypeFreak(5, "AAA6", "ccc"),
                    newTypeFreak(6, "AAA7", "ccc"),
                    newTypeFreak(7, "AAA8", "ccc")
            );

            assertThat(db.typeFreaks().findWithEmbeddedANotIn(asList("AAA1", "AAA3", "AAA7", "AAA8"))).containsExactlyInAnyOrder(
                    newTypeFreak(1, "AAA2", "bbb"),
                    newTypeFreak(3, "AAA4", "bbb"),
                    newTypeFreak(4, "AAA5", "ccc"),
                    newTypeFreak(5, "AAA6", "ccc")
            );
        });
    }

    @Test
    public void findByPredicateWithLimit() {
        List<Project> named = IntStream.range(0, 100)
                .mapToObj(i -> new Project(new Project.Id("named-p" + i), "P" + i))
                .collect(toList());

        db.tx(() -> {
            db.projects().insert(new Project(new Project.Id("unnamed-p1"), null));
            db.projects().insertAll(named);
        });

        db.tx(() -> {
            final List<Project> topNamed = db.projects().findTopNamed(10);
            assertThat(topNamed).hasSize(10);
        });
    }

    protected TypeFreak newTypeFreak(int n, String a, String b) {
        return new TypeFreak(new TypeFreak.Id("tf", n),
                true,
                (byte) 1,
                (byte) 3,
                (short) 2,
                1 << 30,
                1L << 50,
                1.5f,
                0.5,

                true,
                (byte) 1,
                (byte) 255,
                (short) 2,
                1 << 30,
                1L << 50,
                1.5f,
                0.5,

                "\uD83D\uDE09 \u26FE \u262D \u2603 \uD83D\uDCBB",
                "some string",
                "byte string".getBytes(),

                TypeFreak.Status.OK,
                TypeFreak.Status.DRAFT,

                new Embedded(new A(a), new B(b)),
                new Embedded(new A(b), new B(a)),
                new Embedded(new A(b), new B(a)),
                new Embedded(new A(b), new B(a)),
                new Embedded(new A(b), new B(a)),

                Instant.parse("2018-02-05T14:36:05.960Z"),

                asList("1", "2", "3"),
                asList(new Embedded(new A("aaa"), new B("bbb")), new Embedded(new A("xxx"), new B("yyy"))),
                singleton(new Embedded(new A("aaa"), new B("bbb"))),
                singletonMap(1, new Embedded(new A("mmm"), new B("nnn"))),
                singletonMap(1, new Embedded(new A("nnn"), new B("ooo"))),
                singletonMap(1, new Embedded(new A("ooo"), new B("ppp"))),
                singletonMap(1, new Embedded(new A("ppp"), new B("qqq"))),

                new TypeFreak.StringValueWrapper("the string value wrapper"),

                "hi there",

                new TypeFreak.Ticket("CLOUD", 25)
        );
    }

    @Test
    public void simpleCrudInTheSameTx() {
        db.tx(() -> {
            Project p1 = new Project(new Project.Id("1"), "named");
            db.projects().save(p1); // save() is pending until end of tx

            Project p2 = db.projects().find(new Project.Id("1"));
            assertEquals(new Project(new Project.Id("1"), "named"), p2);

            Project p3 = new Project(new Project.Id("1"), "renamed");
            db.projects().save(p3); // save() is pending

            Project p4 = db.projects().find(new Project.Id("1"));
            assertEquals(new Project(new Project.Id("1"), "renamed"), p4);
        });
        db.tx(() -> {
            Project p1 = db.projects().find(new Project.Id("1"));
            assertEquals(new Project(new Project.Id("1"), "renamed"), p1);
            db.projects().delete(new Project.Id("1")); // delete() is pending

            Project p5 = db.projects().find(new Project.Id("1"));
            assertNull(p5); // now it's deleted
        });
        db.tx(() -> {
            Project p1 = db.projects().find(new Project.Id("1"));
            assertNull(p1); // now it's deleted
        });
    }

    @Test
    public void optimisticLockWrite() {
        RepositoryTransaction tx1 = startTransaction();
        tx1.table(Project.class).save(new Project(new Project.Id("1"), "x"));
        tx1.commit();

        RepositoryTransaction tx2 = startTransaction();
        RepositoryTransaction tx3 = startTransaction();
        Project p2 = tx2.table(Project.class).find(new Project.Id("1"));
        Project p3 = tx3.table(Project.class).find(new Project.Id("1"));
        tx2.table(Project.class).save(new Project(new Project.Id("1"), p2.getName() + "y"));
        tx3.table(Project.class).save(new Project(new Project.Id("1"), p3.getName() + "z"));
        tx2.commit();
        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(tx3::commit);

        Project p4 = db.tx(() -> db.table(Project.class).find(new Project.Id("1")));
        assertEquals(new Project(new Project.Id("1"), p2.getName() + "y"), p4);
    }

    @Test
    public void optimisticLockRead() {
        RepositoryTransaction tx0 = startTransaction();
        tx0.table(Project.class).save(new Project(new Project.Id("1"), "p1"));
        tx0.table(Project.class).save(new Project(new Project.Id("2"), "p2"));
        tx0.commit();

        RepositoryTransaction tx1 = startTransaction();
        tx1.table(Project.class).insert(new Project(new Project.Id("3"), "p3"));
        tx1.table(Project.class).find(new Project.Id("1"));
        {
            RepositoryTransaction tx2 = startTransaction();
            tx2.table(Project.class).save(new Project(new Project.Id("1"), "p1-1"));
            tx2.commit();
        }
        // read object was touched -> rollback on any operation
        //не prepare запросы валятся при обращении
        //prepare запросы валятся на комите
        assertThatExceptionOfType(OptimisticLockException.class)
                .isThrownBy(() -> {
                    //не prepare запросы валятся при обращении
                    try {
                        tx1.table(Project.class).find(new Project.Id("1"));
                    } catch (Exception e) {
                        tx1.rollback();
                        throw e;
                    }
                    //prepare запросы валятся на комите
                    tx1.commit();
                });
    }

    @Test
    public void concurrentWriteIsOk() {
        RepositoryTransaction tx1 = startTransaction();
        RepositoryTransaction tx2 = startTransaction();
        tx1.table(Project.class).save(new Project(new Project.Id("1"), "x"));
        tx2.table(Project.class).save(new Project(new Project.Id("1"), "y"));
        tx1.commit();
        tx2.commit();
        assertEquals(new Project(new Project.Id("1"), "y"), db.tx(() -> db.table(Project.class).find(new Project.Id("1"))));
    }

    @Test
    public void concurrentReadIsOk() {
        RepositoryTransaction tx0 = startTransaction();
        tx0.table(Project.class).save(new Project(new Project.Id("1"), "p1"));
        tx0.commit();

        RepositoryTransaction tx1 = startTransaction();
        RepositoryTransaction tx2 = startTransaction();
        tx1.table(Project.class).find(new Project.Id("1"));
        tx2.table(Project.class).find(new Project.Id("1"));
        tx1.commit();
        tx2.commit();
    }

    @Test
    public void allTypes() {
        TypeFreak t1 = db.tx(() -> db.typeFreaks().save(newTypeFreak(1, "aaa", "bbb")));
        TypeFreak t2 = db.tx(() -> db.typeFreaks().find(new TypeFreak.Id("tf", 1)));
        assertEquals(t1, t2);
    }

    @Test
    public void allTypesNull() {
        TypeFreak t1 = db.tx(() -> {
            TypeFreak t = new TypeFreak(new TypeFreak.Id("x", 1),
                    true,
                    (byte) 111,
                    (byte) 111,
                    (short) 22222,
                    1 << 30,
                    1L << 50,
                    1.5f,
                    0.5,

                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    singletonMap(1, new Embedded(new A("a"), new B("b"))),
                    singletonMap(1, new Embedded(new A("a"), new B("b"))),
                    singletonMap(1, new Embedded(new A("a"), new B("b"))),
                    singletonMap(1, new Embedded(new A("a"), new B("b"))),
                    null,
                    null,
                    null
            );
            db.typeFreaks().save(t);
            return t;
        });
        TypeFreak t2 = db.tx(() -> db.typeFreaks().find(new TypeFreak.Id("x", 1)));
        assertEquals(t1, t2);
    }

    @Test
    public void allTypesWithNulls() {
        TypeFreak t1 = db.tx(() -> {
            TypeFreak t = new TypeFreak(new TypeFreak.Id("x", 1),
                    true,
                    (byte) 111,
                    (byte) 111,
                    (short) 22222,
                    1 << 30,
                    1L << 50,
                    1.5f,
                    0.5,

                    null,
                    null,
                    null,
                    null,
                    null,
                    1L >> 20,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Instant.now().truncatedTo(MILLIS), // we store ms for an java.time.Instant
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "hi",
                    null
            );
            db.typeFreaks().save(t);
            return t;
        });
        TypeFreak t2 = db.tx(() -> db.typeFreaks().find(new TypeFreak.Id("x", 1)));
        assertEquals(t1, t2);
    }

    @Test
    public void refs() {
        Project p1 = new Project(new Project.Id("1"), "name1");
        Project p2 = new Project(new Project.Id("2"), "name2");
        Complex c1 = new Complex(new Complex.Id(1, 2L, "3", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(2, 2L, "3", Complex.Status.OK));
        Referring r = new Referring(new Referring.Id("1"),
                p1.getId(),
                c1.getId(),
                asList(p1.getId(), p2.getId()),
                asList(c1.getId(), c2.getId())
        );
        db.tx(() -> {
            db.projects().save(p1);
            db.projects().save(p2);
            db.complexes().save(c1);
            db.complexes().save(c2);
            db.referrings().save(r);
        });
        db.tx(() -> {
            assertEquals(r, db.referrings().find(r.getId()));
            assertEquals(p1, r.getProject().resolve());
            assertEquals(c1, r.getComplex().resolve());
            assertEquals(c1, r.getComplex().resolve());
            assertEquals(asList(p1, p2), r.getProjects().stream().map(Entity.Id::resolve).collect(toList()));
            assertEquals(asList(c1, c2), r.getComplexes().stream().map(Entity.Id::resolve).collect(toList()));
        });
    }

    @Test
    public void severalWriteOperationsInTX() {
        Complex.Id complexId = new Complex.Id(1, 1L, "c", Complex.Status.OK);

        db.tx(() -> {
            db.projects().save(new Project(new Project.Id("1"), "p11"));
            db.projects().save(new Project(new Project.Id("2"), "p2"));
            db.projects().save(new Project(new Project.Id("1"), "p12"));
            db.complexes().save(new Complex(complexId));

            db.projects().delete(new Project.Id("1"));
            db.complexes().delete(complexId);
            db.projects().delete(new Project.Id("2"));
        });

        db.tx(() -> {
            assertNull(db.projects().find(new Project.Id("1")));
            assertNull(db.projects().find(new Project.Id("2")));
            assertNull(db.complexes().find(complexId));
        });
    }

    @Test
    public void insert() {
        db.tx(() -> {
            db.projects().insert(new Project(new Project.Id("unnamed-p1"), null));
            db.projects().insert(new Project(new Project.Id("named-p2"), "P2"));
            db.projects().insert(new Project(new Project.Id("named-p3"), "P3"));
        });
        Complex.Id complexId = new Complex.Id(1, 1L, "c", Complex.Status.OK);
        Project p1 = new Project(new Project.Id("1"), "p1");
        Complex complex = new Complex(complexId);

        db.tx(() -> {
            db.projects().insert(p1);
            db.complexes().insert(complex);
        });

        db.tx(() -> {
            assertEquals(p1, db.projects().find(new Project.Id("1")));
            assertEquals(complex, db.complexes().find(complexId));
        });
        assertThatExceptionOfType(EntityAlreadyExistsException.class)
                .isThrownBy(() -> db.tx(() -> {
                    db.projects().insert(p1);
                }));

        assertThatExceptionOfType(EntityAlreadyExistsException.class)
                .isThrownBy(() -> db.tx(() -> {
                    db.complexes().insert(complex);
                }));

    }

    @Test
    public void alreadyExistsOnCommit() {
        Project p1 = new Project(new Project.Id("1"), "p1");
        Project p12 = new Project(new Project.Id("1"), "p2");

        //this is a problem for one tx.
        assertThatExceptionOfType(EntityAlreadyExistsException.class)
                .isThrownBy(() -> db.tx(() -> {
                    db.projects().insert(p1);
                    //this is a problem for one tx.
                    db.projects().insert(p12);
                }));

        db.tx(() -> db.projects().insert(p1));

        AtomicBoolean executed = new AtomicBoolean(false);
        //already exists only on commit
        assertThatExceptionOfType(EntityAlreadyExistsException.class)
                .isThrownBy(() -> db.tx(() -> {
                    db.projects().insert(p1);
                    //already exists only on commit
                    executed.set(true);
                }));

        assertTrue(executed.get());
    }

    @Test
    public void updateSimpleFieldById() {
        db.tx(() -> db.projects().insert(new Project(new Project.Id("1"), "p1")));

        db.tx(() -> db.projects().updateName(new Project.Id("1"), "NEW-P1-NAME"));

        db.tx(() -> {
            assertThat(db.projects().find(new Project.Id("1")).getName()).isEqualTo("NEW-P1-NAME");
        });
    }

    @Test
    public void updateComplexFieldByComplexId() {
        TypeFreak tf = newTypeFreak(100500, "AAA", "BBB");
        db.tx(() -> db.typeFreaks().insert(tf));

        Embedded newEmbedded = new Embedded(new A("ZZZ"), new B("YYY"));
        db.tx(() -> db.typeFreaks().updateEmbedded(tf.getId(), newEmbedded));

        db.tx(() -> {
            assertThat(db.typeFreaks().find(tf.getId()).getEmbedded()).isEqualTo(newEmbedded);
        });
    }

    @Test
    public void findByComplexIdUsingPredicates() {
        db.tx(() -> db.typeFreaks().insert(
                newTypeFreak(0, "AAA1", "bbb"),
                newTypeFreak(1, "AAA2", "bbb")));

        db.tx(() -> {
            assertThat(db.typeFreaks().findByPredicateWithComplexId(new TypeFreak.Id("tf", 0)))
                    .isEqualTo(newTypeFreak(0, "AAA1", "bbb"));
        });
    }

    @Test
    public void findByManySimpleIdsUsingPredicates() {
        Project projA = new Project(new Project.Id("A"), "aaa");
        Project projB = new Project(new Project.Id("B"), "bbb");
        Project proj0 = new Project(new Project.Id("0"), "000");
        db.tx(() -> db.projects().insert(projA, projB, proj0));

        db.tx(() -> {
            assertThat(db.projects().findByPredicateWithManyIds(ImmutableSet.of(new Project.Id("A"), new Project.Id("B"))))
                    .containsOnly(projA, projB);
            assertThat(db.projects().findByPredicateWithManyIdValues(ImmutableSet.of("A", "B")))
                    .containsOnly(projA, projB);
        });
    }

    @Test
    public void findViewById() {
        TypeFreak tf1 = newTypeFreak(0, "AAA1", "bbb");
        db.tx(() -> db.typeFreaks().insert(
                tf1,
                newTypeFreak(1, "AAA2", "bbb")));

        db.tx(() -> {
            TypeFreak.View found = db.typeFreaks().find(TypeFreak.View.class, tf1.getId());
            assertThat(found).isEqualTo(new TypeFreak.View(tf1.getId(), tf1.getEmbedded()));
        });
    }

    @Test
    public void findViewTypeConversion() {
        TypeFreak tf1 = newTypeFreak(0, "AAA1", "bbb");
        db.tx(() -> db.typeFreaks().insert(
                tf1,
                newTypeFreak(1, "AAA2", "bbb")));

        db.tx(() -> {
            TypeFreak.StringView found = db.typeFreaks().find(TypeFreak.StringView.class, tf1.getId());
            assertThat(found.getId()).isEqualTo(tf1.getId());
            // ...sure looks like JSON to me:
            assertThat(found.getStringEmbedded()).isNotNull();
            assertThat(found.getStringEmbedded().trim()).startsWith("{");
            assertThat(found.getStringEmbedded().trim()).endsWith("}");
        });
    }

    @Test
    public void findViewByIdRange() {
        List<TypeFreak> toInsert = new ArrayList<>();
        List<TypeFreak.View> expectedViews = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TypeFreak tf = newTypeFreak(i, "AAA" + (i + 1), "bbb");
            toInsert.add(tf);
            if (i < 50) {
                expectedViews.add(new TypeFreak.View(tf.getId(), tf.getEmbedded()));
            }
        }
        Range<TypeFreak.Id> findRange = new Range<>(
                expectedViews.get(0).getId(),
                expectedViews.get(expectedViews.size() - 1).getId()
        );

        db.tx(() -> db.typeFreaks().insertAll(toInsert));

        db.tx(() -> {
            List<TypeFreak.View> found = db.typeFreaks().find(TypeFreak.View.class, findRange);
            assertThat(found).containsExactlyInAnyOrderElementsOf(expectedViews);
        });
    }

    @Test
    public void findAllViews() {
        List<TypeFreak> toInsert = new ArrayList<>();
        List<TypeFreak.View> expectedViews = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TypeFreak tf = newTypeFreak(i, "AAA" + (i + 1), "bbb");
            toInsert.add(tf);
            expectedViews.add(new TypeFreak.View(tf.getId(), tf.getEmbedded()));
        }

        db.tx(() -> db.typeFreaks().insertAll(toInsert));

        db.tx(() -> {
            List<TypeFreak.View> found = db.typeFreaks().findAll(TypeFreak.View.class);
            assertThat(found).containsExactlyInAnyOrderElementsOf(expectedViews);
        });
    }

    @Test
    public void findViewByPredicate() {
        List<TypeFreak> toInsert = new ArrayList<>();
        List<TypeFreak.View> toFind = new ArrayList<>();
        List<String> embeddedAsToFind = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String embeddedA = "AAA" + (i + 1);
            TypeFreak tf = newTypeFreak(i, embeddedA, "bbb");

            toInsert.add(tf);
            toFind.add(new TypeFreak.View(tf.getId(), tf.getEmbedded()));
            embeddedAsToFind.add(embeddedA);
        }

        db.tx(() -> db.typeFreaks().insertAll(toInsert));

        db.tx(() -> {
            List<TypeFreak.View> found = db.typeFreaks().findViewWithEmbeddedAIn(embeddedAsToFind);
            assertThat(found).containsExactlyInAnyOrderElementsOf(toFind);
        });
    }

    @Test
    public void findRangeWithPrimitiveId() {
        db.tx(() -> db.primitives().insert(
                new Primitive(new Primitive.Id(1), 100500),
                new Primitive(new Primitive.Id(42), 9000),
                new Primitive(new Primitive.Id(-100500), 0)
        ));

        db.tx(() -> {
            assertThat(db.primitives().find(new Range<>(new Primitive.Id(-5), new Primitive.Id(50))))
                    .containsOnly(
                            new Primitive(new Primitive.Id(1), 100500),
                            new Primitive(new Primitive.Id(42), 9000)
                    );
        });
    }

    @Test
    public void findAllResultsAreOrderedByIdAscending() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.FAIL));
        Complex c3 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_999, 0L, "UUU", Complex.Status.OK));
        Complex c5 = new Complex(new Complex.Id(999_000, 0L, "UUU", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4, c5));

        List<Complex> found = db.tx(() -> db.complexes().findAll());
        assertThat(found).containsExactly(c5, c4, c3, c2, c1);
    }

    @Test
    public void findByRangeResultsAreOrderedByIdAscending() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.FAIL));
        Complex c3 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_999, 0L, "UUU", Complex.Status.OK));
        Complex c5 = new Complex(new Complex.Id(999_000, 0L, "UUU", Complex.Status.OK));
        Complex c6 = new Complex(new Complex.Id(999_000, 0L, "AAA", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4, c5, c6));

        List<Complex> found = db.tx(() -> db.complexes().find(
                new Range<>(
                        new Complex.Id(999_000, 0L, "AAA", null),
                        new Complex.Id(999_000, 0L, "UUU", null)
                )));
        assertThat(found).containsExactly(c6, c5);
    }

    @Test
    public void findByPredicateResultsAreOrderedByIdAscending() {
        db.tx(() -> db.projects().insert(
                new Project(new Project.Id("named-p3"), "P3"),
                new Project(new Project.Id("unnamed-p1"), null),
                new Project(new Project.Id("named-p2"), "P2")
        ));

        assertThat(db.tx(() -> db.projects().findNamed())).containsExactly(
                new Project(new Project.Id("named-p2"), "P2"),
                new Project(new Project.Id("named-p3"), "P3")
        );
    }

    @Test
    public void projections() {
        db.tx(() -> {
            db.table(Book.class).save(new Book(new Book.Id("1"), 1, "title1", List.of("author1")));
            db.table(Book.class).save(new Book(new Book.Id("2"), 1, "title2", List.of("author2")));
            db.table(Book.class).save(new Book(new Book.Id("3"), 1, null, List.of("author1", "author2")));
            db.table(Book.class).save(new Book(new Book.Id("4"), 1, "title1", List.of()));
        });

        assertThat(db.tx(() -> db.table(Book.ByTitle.class).countAll()))
                .isEqualTo(3L);
        assertThat(db.tx(() -> db.table(Book.ByTitle.class).find(new Range<>(new Book.ByTitle.Id("title1", null)))))
                .hasSize(2);
        assertThat(db.tx(() -> db.table(Book.ByTitle.class).find(new Range<>(new Book.ByTitle.Id("title2", null)))))
                .hasSize(1);

        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).countAll()))
                .isEqualTo(4L);
        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).find(new Range<>(new Book.ByAuthor.Id("author1", null)))))
                .hasSize(2);
        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).find(new Range<>(new Book.ByAuthor.Id("author2", null)))))
                .hasSize(2);

        db.tx(() -> {
            db.table(Book.class).modifyIfPresent(new Book.Id("1"), b -> b.updateTitle("title2"));
            db.table(Book.class).modifyIfPresent(new Book.Id("2"), b -> b.updateTitle(null));
            db.table(Book.class).modifyIfPresent(new Book.Id("3"), b -> b.withAuthors(List.of("author2")));
            db.table(Book.class).modifyIfPresent(new Book.Id("4"), b -> b.withAuthors(List.of("author1", "author2")));
        });

        assertThat(db.tx(() -> db.table(Book.ByTitle.class).countAll()))
                .isEqualTo(2L);
        assertThat(db.tx(() -> db.table(Book.ByTitle.class).find(new Range<>(new Book.ByTitle.Id("title1", null)))))
                .hasSize(1);
        assertThat(db.tx(() -> db.table(Book.ByTitle.class).find(new Range<>(new Book.ByTitle.Id("title2", null)))))
                .hasSize(1);

        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).countAll()))
                .isEqualTo(5L);
        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).find(new Range<>(new Book.ByAuthor.Id("author1", null)))))
                .hasSize(2);
        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).find(new Range<>(new Book.ByAuthor.Id("author2", null)))))
                .hasSize(3);

        db.tx(() -> db.table(Book.class).findAll().forEach(b -> db.table(Book.class).delete(b.getId())));
        assertThat(db.tx(() -> db.table(Book.ByTitle.class).countAll()))
                .isEqualTo(0L);
        assertThat(db.tx(() -> db.table(Book.ByAuthor.class).countAll()))
                .isEqualTo(0L);
    }

    /**
     * {@link #parallelTx(boolean, boolean, Consumer)} make two tx.
     * In first  - read from table (see consumers - findAll, findId, findRange)
     * In second - insert Complex (1, 2L, "c", Complex.Status.OK)
     * In first - again read same consumer
     */
    @Test
    public void rangeLock() {
        parallelTx(true, true, Table::findAll); // lock on table
        parallelTx(false, false, Table::findAll); // lock on table

        parallelTx(true, true, t -> t.find(new Id(1, 2L, "c", Complex.Status.OK))); // lock on row
        parallelTx(false, false, t -> t.find(new Id(1, 2L, "c", Complex.Status.OK))); // lock on row

        parallelTx(false, true, t -> t.find(new Complex.Id(2, 4L, "l", Complex.Status.FAIL))); //not lock on another row
        parallelTx(false, false, t -> t.find(new Complex.Id(2, 4L, "l", Complex.Status.FAIL))); //not lock on another row

        parallelTx(true, true, t -> t.find(new Range<>(new Complex.Id(1, 2L, null, null)))); //lock on range
        parallelTx(false, false, t -> t.find(new Range<>(new Complex.Id(1, 2L, null, null)))); //lock on range

        parallelTx(false, true, t -> t.find(new Range<>(new Complex.Id(2, 2L, null, null)))); //not lock on another range
        parallelTx(false, false, t -> t.find(new Range<>(new Complex.Id(2, 2L, null, null)))); //not lock on another range
    }

    private void parallelTx(boolean shouldThrown, boolean writeTx, Consumer<Table<Complex>> consumer) {
        RepositoryTransaction tx = startTransaction();
        if (writeTx) {
            tx.table(Book.class).save(new Book(new Book.Id("1"), 1, "title1", List.of("author1")));
        }
        consumer.accept(tx.table(Complex.class));

        db.tx(() -> db.complexes().insert(new Complex(new Complex.Id(1, 2L, "c", Complex.Status.OK))));

        Runnable runnable = () -> {
            try {
                consumer.accept(tx.table(Complex.class));
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
            tx.commit();
        };
        if (shouldThrown) {
            assertThatExceptionOfType(OptimisticLockException.class).isThrownBy(runnable::run);
        } else {
            runnable.run();
        }

        db.tx(() -> db.complexes().deleteAll());
    }

    @Test
    public void businessExceptionInTx() {
        class BusinessException extends RuntimeException {
        }

        assertThatExceptionOfType(BusinessException.class).isThrownBy(() -> db.tx(() -> {
            db.primitives().find(new Primitive.Id(25));
            throw new BusinessException();
        }));
    }

    @Test
    public void readOnlyTransaction() {
        assertThatExceptionOfType(IllegalTransactionIsolationLevelException.class)
                .isThrownBy(() -> db.readOnly().run(() -> db.projects().save(new Project(new Project.Id("13"), "p13"))))
                .withMessage("Mutable operations are not allowed for isolation level ONLINE_CONSISTENT_READ_ONLY");
    }

    @Test
    public void ctorValidationFailure() {
        EntityWithValidation goodValue = new EntityWithValidation(new EntityWithValidation.Id("hey"), 43L);
        db.tx(() -> {
            db.entitiesWithValidation().save(goodValue);
            db.entitiesWithValidation().save(EntityWithValidation.BAD_VALUE);
        });

        assertThat(db.tx(() -> db.entitiesWithValidation().find(goodValue.getId())))
                .isEqualTo(goodValue);
        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() -> db.tx(() -> db.entitiesWithValidation().find(EntityWithValidation.BAD_VALUE.getId())));
    }

    @Test
    public void viewCtorValidationFailure() {
        EntityWithValidation goodValue = new EntityWithValidation(new EntityWithValidation.Id("hey"), 43L);
        db.tx(() -> {
            db.entitiesWithValidation().save(goodValue);
            db.entitiesWithValidation().save(EntityWithValidation.BAD_VALUE_IN_VIEW);
        });

        assertThat(db.tx(() -> db.entitiesWithValidation().find(EntityWithValidation.OnlyVal.class, goodValue.getId())))
                .isEqualTo(new EntityWithValidation.OnlyVal(goodValue.getValue()));
        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() -> db.tx(() -> db.entitiesWithValidation()
                        .find(EntityWithValidation.OnlyVal.class, EntityWithValidation.BAD_VALUE_IN_VIEW.getId())));
    }

    @Test
    public void complexIdEquals() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 42L, "ZZZ", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 76L, "ZZZ", Complex.Status.OK));

        db.tx(() -> db.complexes().insert(c1, c2, c3));

        assertThat(
                db.tx(() -> db.complexes().query()
                        .where("id").eq(c1.getId())
                        .orderBy(ob -> ob.orderBy("id").ascending())
                        .find())
        ).containsExactly(c1);
    }

    @Test
    public void complexIdNotEquals() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 42L, "ZZZ", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 76L, "ZZZ", Complex.Status.OK));

        db.tx(() -> db.complexes().insert(c1, c2, c3));

        assertThat(
                db.tx(() -> db.complexes().query()
                        .where("id").neq(c1.getId())
                        .orderBy(ob -> ob.orderBy("id").ascending())
                        .find())
        ).containsExactly(c2, c3);
    }

    @Test
    public void complexIdGreaterThan() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 42L, "ZZZ", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 76L, "ZZZ", Complex.Status.OK));

        db.tx(() -> db.complexes().insert(c1, c2, c3));

        assertThat(
                db.tx(() -> db.complexes().query()
                        .where("id").gt(c1.getId())
                        .orderBy(ob -> ob.orderBy("id").ascending())
                        .find())
        ).containsExactly(c2, c3);
    }

    @Test
    public void complexIdLessThan() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 42L, "ZZZ", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 76L, "ZZZ", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3));

        assertThat(
                db.tx(() -> db.complexes().query()
                        .where("id").lt(c3.getId())
                        .orderBy(ob -> ob.orderBy("id").descending())
                        .find())
        ).containsExactly(c2, c1);
    }

    @Test
    public void byteIdLessThan() {
        BytePkEntity e0 = BytePkEntity.valueOf();
        BytePkEntity e1 = BytePkEntity.valueOf(1, 2, 3);
        BytePkEntity e2 = BytePkEntity.valueOf(1, 2, 4);
        BytePkEntity e3 = BytePkEntity.valueOf(1, 3, 255);
        db.tx(() -> db.bytePkEntities().insert(e0, e1, e2, e3));

        assertThat(db.tx(() -> db.bytePkEntities().query()
                .where("id").lt(e3.getId())
                .orderBy(ob -> ob.orderBy("id").descending())
                .find()
        )).containsExactly(e2, e1, e0);
    }

    @Test
    public void byteIdGreaterThan() {
        BytePkEntity e0 = BytePkEntity.valueOf();
        BytePkEntity e1 = BytePkEntity.valueOf(1, 2, 3);
        BytePkEntity e2 = BytePkEntity.valueOf(1, 2, 4);
        BytePkEntity e3 = BytePkEntity.valueOf(1, 3, 255);
        db.tx(() -> db.bytePkEntities().insert(e0, e1, e2, e3));

        assertThat(db.tx(() -> db.bytePkEntities().query()
                .where("id").gt(e1.getId())
                .orderBy(ob -> ob.orderBy("id").ascending())
                .find()
        )).containsExactly(e2, e3);
    }

    @Test
    public void byteIdEmpty() {
        BytePkEntity e0 = BytePkEntity.valueOf();
        db.tx(() -> db.bytePkEntities().insert(e0));

        assertThat(db.tx(() -> db.bytePkEntities().find(e0.getId()))).isEqualTo(e0);
    }

    @Test
    public void complexIdLessThanWithEmbeddedId() {
        Supabubble sa = new Supabubble(new Supabubble.Id(new Project.Id("naher"), "bubble-A"));
        Supabubble sb = new Supabubble(new Supabubble.Id(new Project.Id("naher"), "bubble-B"));
        Supabubble sc = new Supabubble(new Supabubble.Id(new Project.Id("naher"), "bubble-C"));
        db.tx(() -> db.supabubbles().insert(sa, sb, sc));

        assertThat(
                db.tx(() -> db.supabubbles().query()
                        .where("id").lt(sc.getId())
                        .orderBy(ob -> ob.orderBy("id").descending())
                        .find())
        ).containsExactly(sb, sa);
    }

    @Test
    public void checkCanMergeWorkProperly() {
        db.tx(() -> {
            Project p1 = new Project(new Project.Id("1"), "first");
            Project p2 = new Project(new Project.Id("2"), "second");

            db.projects().insert(p1);
            db.projects().delete(p2.getId());
            db.projects().insert(p2);
        });
    }


    @Test
    public void doMultipleSaveInOneTx() {
        Project p1 = new Project(new Project.Id("1"), "first");
        Project p2 = new Project(new Project.Id("2"), "second");

        db.tx(() -> {
            db.projects().save(p1);
            db.projects().save(p2);
        });

        assertThat(db.tx(() -> db.projects().find(p1.getId()))).isEqualTo(p1);
        assertThat(db.tx(() -> db.projects().find(p2.getId()))).isEqualTo(p2);
    }

    @Test
    public void deleteInsertInOneTx() {
        // merged to upsert, integration test, that's merge correct
        Project p1 = new Project(new Project.Id("1"), "project1");
        Project p2 = new Project(new Project.Id("2"), "project2");
        db.tx(() -> {
            db.projects().delete(p1.getId());
            db.projects().insert(p1);
            db.projects().delete(p2.getId());
            db.projects().insert(p2);
        });

        assertThat(db.tx(() -> db.projects().find(p1.getId()))).isEqualTo(p1);
        assertThat(db.tx(() -> db.projects().find(p2.getId()))).isEqualTo(p2);
    }

    @Test
    public void multistatementReadonlyTransaction() {
        Project p1 = new Project(new Project.Id("1"), "first");
        Project p2 = new Project(new Project.Id("2"), "second");

        db.tx(() -> {
            db.projects().save(p1);
            db.projects().save(p2);
        });
        RepositoryTransaction tx = repository.startTransaction(TxOptions.create(IsolationLevel.STALE_CONSISTENT_READ_ONLY));
        assertThat(tx.table(Project.class).find(p1.getId())).isNotNull();
        assertThat(tx.table(Project.class).find(p2.getId())).isNotNull();
    }

    @Test
    public void noOptimisticLockOnScan() {
        RepositoryTransaction tx0 = repository.startTransaction(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE));
        tx0.table(Project.class).save(new Project(new Project.Id("1"), "p1"));
        tx0.table(Project.class).save(new Project(new Project.Id("2"), "p2"));
        tx0.commit();

        TxOptions txOptions = TxOptions.create(IsolationLevel.STALE_CONSISTENT_READ_ONLY)
                .withScanOptions(TxOptions.ScanOptions.DEFAULT);
        RepositoryTransaction tx1 = repository.startTransaction(txOptions);
        tx1.table(Project.class).find(new Project.Id("1"));
        {
            RepositoryTransaction tx2 = repository.startTransaction(TxOptions.create(IsolationLevel.SERIALIZABLE_READ_WRITE));
            tx2.table(Project.class).save(new Project(new Project.Id("1"), "p1-1"));
            tx2.commit();
        }

        //не prepare запросы не валятся при обращении
        try {
            tx1.table(Project.class).find(new Project.Id("1"));
        } catch (Exception e) {
            tx1.rollback();
            throw e;
        }
        //prepare запросы не валятся на комите
        tx1.commit();
    }


    @Test
    public void findEntityAndViewWithTheSameKey() {
        TypeFreak tf1 = newTypeFreak(0, "AAA1", "bbb");
        db.tx(() -> db.typeFreaks().insert(tf1));

        // find view by id and than find entity by id
        db.tx(() -> {
            TypeFreak.View foundView = db.typeFreaks().find(TypeFreak.View.class, tf1.getId());
            assertThat(foundView).isEqualTo(new TypeFreak.View(tf1.getId(), tf1.getEmbedded()));

            TypeFreak foundTF = db.typeFreaks().find(tf1.getId());
            assertThat(foundTF).isEqualTo(tf1);
        });

        // find entity by id and than find view by id
        db.tx(() -> {
            TypeFreak foundTF = db.typeFreaks().find(tf1.getId());
            assertThat(foundTF).isEqualTo(tf1);

            TypeFreak.View foundView = db.typeFreaks().find(TypeFreak.View.class, tf1.getId());
            assertThat(foundView).isEqualTo(new TypeFreak.View(tf1.getId(), tf1.getEmbedded()));
        });
    }

    @Test
    public void scanUpdateFails() {
        Assertions.assertThatExceptionOfType(IllegalTransactionScanException.class)
                .isThrownBy(() -> db.scan().run(() -> {
                    db.projects().save(new Project(new Project.Id("1"), "p1"));
                }));
    }

    @Test
    public void scanNotTruncated() {
        int maxPageSizeBiggerThatReal = 11_000;

        db.tx(() -> IntStream.range(0, maxPageSizeBiggerThatReal).forEach(
                i -> db.projects().save(new Project(new Project.Id("id_" + i), "name"))
        ));

        List<Project> result = db.scan().withMaxSize(maxPageSizeBiggerThatReal).run(() -> db.projects().findAll());
        assertEquals(maxPageSizeBiggerThatReal, result.size());
    }

    @Test
    public void scanFind() {
        Project p1 = new Project(new Project.Id("1"), "p1");

        db.tx(() -> {
            db.projects().save(p1);
        });

        Project result = db.scan().run(() -> db.projects().find(p1.getId()));
        assertEquals(p1, result);
    }

    @Test
    public void scanStreamAll() {
        int size = 10;

        db.tx(() -> IntStream.range(0, size).forEach(
                i -> db.projects().save(new Project(new Project.Id("id_" + i), "name"))
        ));

        List<Project> result = db.scan().run(() -> db.projects().streamAll(1).collect(toList()));
        assertEquals(size, result.size());
    }

    @Test
    public void businessExceptionInScanTx() {
        class BusinessException extends RuntimeException {
        }

        assertThatExceptionOfType(BusinessException.class).isThrownBy(() -> db.tx(() -> {
            db.primitives().find(new Primitive.Id(25));
            throw new BusinessException();
        }));
    }

    @Test
    public void throwConversionExceptionOnDeserializationProblem() {
        NonDeserializableEntity nonDeserializableEntity = new NonDeserializableEntity(
                new NonDeserializableEntity.Id("ru-vladimirsky-central-001"),
                new NonDeserializableObject()
        );
        db.tx(() -> db.table(NonDeserializableEntity.class).insert(nonDeserializableEntity));

        assertThatExceptionOfType(ConversionException.class)
                .isThrownBy(() -> db.tx(() -> db.table(NonDeserializableEntity.class).find(nonDeserializableEntity.getId())));
    }

    @Test
    public void throwConversionExceptionOnDeserializationReadTableProblem() {
        NonDeserializableEntity nonDeserializableEntity = new NonDeserializableEntity(
                new NonDeserializableEntity.Id("ru-vladimirsky-central-001"),
                new NonDeserializableObject()
        );
        db.tx(() -> db.table(NonDeserializableEntity.class).insert(nonDeserializableEntity));

        ReadTableParams<NonDeserializableEntity.Id> params = ReadTableParams.<NonDeserializableEntity.Id>builder().useNewSpliterator(true).build();
        assertThatExceptionOfType(ConversionException.class).isThrownBy(() ->
                db.readOnly().run(() -> db.table(NonDeserializableEntity.class).readTable(params).collect(toList()))
        );
    }


    @Test
    public void resolveOnReadTableStream() {
        db.tx(() -> {
            db.projects().save(new Project(new Project.Id("1"), "p1"));
            db.referrings().save(new Referring(new Referring.Id("1"), new Project.Id("1"), null, null, null));
            db.projects().save(new Project(new Project.Id("2"), "p2"));
            db.referrings().save(new Referring(new Referring.Id("2"), new Project.Id("2"), null, null, null));

        });

        ReadTableParams<Referring.Id> params = ReadTableParams.<Referring.Id>builder().useNewSpliterator(true).build();
        db.readOnly().run(() -> db.referrings().readTable(params).forEach(r -> r.getProject().resolve()));
    }

    @Test
    public void customMarshaling() {
        WithUnflattenableField entity = new WithUnflattenableField(
                new WithUnflattenableField.Id("id42"),
                new WithUnflattenableField.Unflattenable("Hello, world!", 100_500)
        );
        db.tx(() -> db.table(WithUnflattenableField.class).insert(entity));

        db.tx(() -> {
            assertThat(db.table(WithUnflattenableField.class).find(entity.getId())).isEqualTo(entity);
        });
    }

    @Test
    public void readFromCache() {
        Complex.Id id = new Complex.Id(1, 2L, "c", Complex.Status.OK);
        db.tx(() -> {
            db.complexes().insert(new Complex(id));
        });
        db.tx(() -> {
            Complex first = db.complexes().find(id);
            Complex second = db.complexes().find(id);

            // Check, that there are the same objects, it's mean they was returned from cache
            assertThat(second).isSameAs(first);
        });
    }

    @Test
    public void findRangeAndPutInCache() {
        db.tx(this::makeComplexes);
        db.tx(() -> {
            List<Complex> rangeResults = db.complexes().find(new Range<>(new Complex.Id(0, 0L, null, null)));
            assertEquals(6, rangeResults.size());

            // Check, that there are the same objects, it's mean they was returned from cache
            for (Complex c : rangeResults) {
                Complex newFound = db.complexes().find(c.getId());
                assertThat(newFound).isSameAs(c);
            }
        });
    }

    @Test
    public void findAllAndPutInCache() {
        db.tx(this::makeComplexes);
        db.tx(() -> {
            List<Complex> rangeResults = db.complexes().findAll();
            assertEquals(54, rangeResults.size());

            // Check, that there are the same objects, it's mean they was returned from cache
            for (Complex c : rangeResults) {
                Complex newFound = db.complexes().find(c.getId());
                assertThat(newFound).isSameAs(c);
            }
        });
    }

    @Test
    public void readAndFailOnInconsistentDataSucceedOnRetry() {
        // 3 entities are possible, one in each of 3 different tables. All are named after 3 famous sith of "Star Wars".
        final Primitive.Id sidiousId = new Primitive.Id(1L);
        final Complex.Id tyranusId = new Complex.Id(2, 0L, "Tyranus", Complex.Status.OK);
        final Project.Id vaderId = new Project.Id("Vader");

        // But 'Always two there are', we consistently keep exactly 2 entities altogether:
        Consumer<RepositoryTransaction> createFirstTwo = (tx) -> {
            tx.table(Primitive.class).insert(new Primitive(sidiousId, 10));
            tx.table(Complex.class).insert(new Complex(tyranusId));
        };

        // Do initial transaction:
        runInTx(createFirstTwo);

        // Prepare altering transaction as a hook that doesn't do anything in the second attempt.
        Runnable concurrentChanger = makeOneShotRunnable(() -> db.separate().tx(() -> {
            db.complexes().delete(tyranusId);
            db.projects().insert(new Project(vaderId, "abc"));
        }));

        List<Void> txAttempts = new ArrayList<>();

        List<Entity<?>> twoElements = db.tx(() -> {
            txAttempts.add(null);

            db.table(Book.class).save(new Book(new Book.Id("1"), 1, "title1", List.of("author1")));

            List<Entity<?>> result = new ArrayList<>();

            result.add(db.complexes().find(tyranusId));

            // This should invalidate the current tx on the first attempt.
            concurrentChanger.run();

            result.add(db.primitives().find(sidiousId));
            result.add(db.projects().find(vaderId));

            result.removeIf(Objects::isNull);

            if (result.size() > 2) {
                throw new RuntimeException("We are seeing inconsistent data, 'always two there are' rule is broken, we found " + result.size());
            }

            return result;
        });

        assertThat(txAttempts).hasSize(2);
        assertThat(twoElements).hasSize(2);
    }

    @Test
    public void stringValuedIdInsert() {
        Map<UpdateFeedEntry.Id, UpdateFeedEntry> inserted = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            var snap = new UpdateFeedEntry(UpdateFeedEntry.Id.generate("insert"), Instant.now(), "payload-" + i);
            db.tx(() -> db.updateFeedEntries().insert(snap));
            inserted.put(snap.getId(), snap);
        }

        assertThat(db.tx(() -> db.updateFeedEntries().find(inserted.keySet())))
                .containsExactlyInAnyOrderElementsOf(inserted.values());

        assertThat(db.tx(() -> db.updateFeedEntries().list(ListRequest.builder(UpdateFeedEntry.class)
                .filter(fb -> fb.where("id").in(inserted.keySet()))
                .build())))
                .containsExactlyInAnyOrderElementsOf(inserted.values());

        for (var e : inserted.entrySet()) {
            assertThat(db.tx(() -> db.updateFeedEntries().query()
                    .filter(fb -> fb.where("id").eq(e.getKey()))
                    .findOne())).isEqualTo(e.getValue());
        }
    }

    protected void runInTx(Consumer<RepositoryTransaction> action) {
        // We do not retry transactions, because we do not expect conflicts in our test scenarios.
        RepositoryTransaction transaction = startTransaction();
        try {
            action.accept(transaction);
        } catch (Throwable t) {
            transaction.rollback();
            throw t;
        }
        transaction.commit();
    }

    private void testList(FilterExpression<Project> filterExpression, Project... expectedProjects) {
        ListRequest<Project> request = ListRequest.builder(Project.class)
                .filter(filterExpression)
                .build();

        ListResult<Project> result = db.readOnly().run(() -> db.projects().list(request));

        Set<Project> expected = new HashSet<>(Arrays.asList(expectedProjects));

        assertEquals(expected, new HashSet<>(result.getEntries()));
    }

    protected static Runnable makeOneShotRunnable(Runnable cmd) {
        return new Runnable() {
            private boolean doneOnce = false;

            @Override
            public void run() {
                if (doneOnce) {
                    return;
                }
                doneOnce = true;
                cmd.run();
            }
        };
    }
}
