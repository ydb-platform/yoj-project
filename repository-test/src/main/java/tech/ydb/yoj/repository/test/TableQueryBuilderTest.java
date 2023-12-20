package tech.ydb.yoj.repository.test;

import lombok.NonNull;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableQueryBuilder;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak.Status;

import java.time.Instant;
import java.util.List;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.databind.expression.FilterBuilder.not;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;

public abstract class TableQueryBuilderTest extends RepositoryTestSupport {
    protected TestDb db;

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

    @Override
    protected final Repository createRepository() {
        return TestEntities.init(createTestRepository());
    }

    protected abstract Repository createTestRepository();

    protected <T extends Entity<T>> TableQueryBuilder<T> createQueryBuilder(@NonNull Class<T> entityClass) {
        return getQueryBuilder(db.table(entityClass));
    }

    @Test
    public void basic() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        Project p3 = new Project(new Project.Id("uuid001"), "ZZZ");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput, p3));

        db.tx(() -> {
            List<Project> page1 = projectQuery()
                    .limit(1)
                    .orderBy(ob -> ob.orderBy("name").descending())
                    .filter(fb -> fb.where("name").in("AAA", "XXX", "ZZZ"))
                    .find();
            Assertions.assertThat(page1).containsExactly(p3);

            List<Project> page2 = projectQuery()
                    .limit(1)
                    .orderBy(ob -> ob.orderBy("name").descending())
                    .filter(fb -> fb.where("name").in("AAA", "XXX", "ZZZ"))
                    .offset(1)
                    .find();
            Assertions.assertThat(page2).containsExactly(p2);

            List<Project> page3 = projectQuery()
                    .limit(1)
                    .orderBy(ob -> ob.orderBy("name").descending())
                    .filter(fb -> fb.where("name").in("AAA", "XXX", "ZZZ"))
                    .offset(2)
                    .find();
            Assertions.assertThat(page3).containsExactly(p1);

            List<Project> page4 = projectQuery()
                    .limit(1)
                    .orderBy(ob -> ob.orderBy("name").descending())
                    .filter(fb -> fb.where("name").in("AAA", "XXX", "ZZZ"))
                    .offset(3)
                    .find();
            Assertions.assertThat(page4).isEmpty();
        });
    }

    @Test
    public void complexIdRange() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 15L, "KKK", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_000, 15L, "AAA", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(3)
                    .filter(fb -> fb.where("id.a").eq(999_999))
                    .find();
            Assertions.assertThat(page).containsExactly(c3, c2, c1);
        });
    }

    @Test
    public void complexIdFullScan() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 15L, "KKK", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_000, 15L, "AAA", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(3)
                    .filter(fb -> fb.where("id.c").eq("UUU"))
                    .find();
            Assertions.assertThat(page).containsExactly(c2);
        });
    }

    @Test
    public void defaultOrderingIsByIdAscending() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 0L, "UUU", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_000, 0L, "UUU", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(4)
                    .find();
            Assertions.assertThat(page).containsExactly(c4, c3, c2, c1);
        });
    }

    @Test
    public void and() {
        Complex c1 = new Complex(new Complex.Id(1, 100L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(1, 200L, "UUU", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(1, 300L, "KKK", Complex.Status.OK));
        Complex notInOutput = new Complex(new Complex.Id(2, 300L, "AAA", Complex.Status.OK));

        db.tx(() -> db.complexes().insert(c1, c2, c3, notInOutput));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(4)
                    .filter(fb -> fb.where("id.a").eq(1).and("id.b").gte(100L).and("id.b").lte(300L))
                    .find();
            Assertions.assertThat(page).containsExactly(c1, c2, c3);
        });
    }

    @Test
    public void enumParsing() {
        db.tx(() -> getQueryBuilder(db.typeFreaks())
                .where("status").eq(Status.DRAFT)
                .orderBy(ob -> ob.orderBy("status").descending())
                .limit(1)
                .find());
    }

    @Test
    public void flattenedIsNull() {
        var tf = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        db.tx(() -> db.typeFreaks().insert(tf));

        List<TypeFreak> lst = db.tx(() -> getQueryBuilder(db.typeFreaks())
                .where("jsonEmbedded").isNull()
                .limit(100)
                .find());
        assertThat(lst).containsOnly(tf);
    }

    @Test
    public void flattenedIsNotNull() {
        var tf = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, new TypeFreak.Embedded(new TypeFreak.A("A"), new TypeFreak.B("B")), null, null, null, null, null, null, null, null, null, null);
        db.tx(() -> db.typeFreaks().insert(tf));

        List<TypeFreak> lst = db.tx(() -> getQueryBuilder(db.typeFreaks())
                .where("jsonEmbedded").isNotNull()
                .limit(100)
                .find());
        assertThat(lst).containsOnly(tf);
    }

    @Test
    public void filterStringValuedByString() {
        TypeFreak typeFreak = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, new TypeFreak.Ticket("CLOUD", 100500));
        db.tx(() -> db.typeFreaks().insert(typeFreak));
        List<TypeFreak> lst = db.tx(() -> getQueryBuilder(db.typeFreaks())
                .filter(fb -> fb.where("ticket").eq("CLOUD-100500"))
                .limit(1)
                .find());

        Assertions.assertThat(lst).containsOnly(typeFreak);
    }

    @Test
    public void filterStringValuedByStruct() {
        TypeFreak.Ticket ticket = new TypeFreak.Ticket("CLOUD", 100500);
        TypeFreak typeFreak = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, ticket);
        db.tx(() -> db.typeFreaks().insert(typeFreak));
        List<TypeFreak> lst = db.tx(() -> getQueryBuilder(db.typeFreaks())
                .filter(newFilterBuilder(TypeFreak.class)
                        .where("ticket").eq(ticket)
                        .build())
                .limit(1)
                .find());

        Assertions.assertThat(lst).containsOnly(typeFreak);
    }

    @Test
    public void embeddedNulls() {
        db.tx(() -> db.typeFreaks().insert(
                new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
        ));
        List<TypeFreak> lst = db.tx(() -> getQueryBuilder(db.typeFreaks())
                .filter(fb -> fb.where("embedded.a.a").eq("myfqdn"))
                .limit(1)
                .find());

        Assertions.assertThat(lst).isEmpty();
    }

    @Test
    public void simpleIdIn() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        Project p3 = new Project(new Project.Id("uuid001"), "ZZZ");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput, p3));

        db.tx(() -> {
            List<Project> page = projectQuery()
                    .limit(100)
                    .filter(fb -> fb.where("id").in("uuid777", "uuid001", "uuid002"))
                    .orderBy(ob -> ob.orderBy("id").ascending())
                    .find();
            Assertions.assertThat(page).containsExactlyInAnyOrder(p1, p2, p3);
        });
    }

    @Test
    public void complexIdIn() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "AAA", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 14L, "BBB", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_000, 13L, "CCC", Complex.Status.FAIL));
        Complex c4 = new Complex(new Complex.Id(999_000, 12L, "DDD", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(100)
                    .filter(fb -> fb
                            .where("id.a").in(999_999,999_000)
                            .and("id.b").in(15L, 13L)
                            .and("id.c").in("AAA", "CCC")
                            .and("id.d").in("OK", "FAIL")
                    )
                    .orderBy(ob -> ob.orderBy("id").descending())
                    .find();
            Assertions.assertThat(page).containsExactly(c1, c3);
        });
    }

    @Test
    public void complexUnixTimestampRelational() {
        Instant now = Instant.now();
        Instant nowPlus1 = now.plusMillis(1L);
        Instant nowPlus2 = now.plusMillis(2L);

        Complex c1 = new Complex(new Complex.Id(999_999, now.toEpochMilli(), "AAA", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, nowPlus1.toEpochMilli(), "BBB", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_000, nowPlus2.toEpochMilli(), "CCC", Complex.Status.FAIL));
        db.tx(() -> db.complexes().insert(c1, c2, c3));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(100)
                    .filter(fb -> fb.where("id.a").in(999_999,999_000).and("id.b").gte(now).and("id.b").lt(nowPlus2))
                    .orderBy(ob -> ob.orderBy("id.a").descending())
                    .find();
            Assertions.assertThat(page).containsExactlyInAnyOrder(c1, c2);
        });
    }

    @Test
    public void complexUnixTimestampIn() {
        Instant now = Instant.now();
        Instant nowPlus1 = now.plusMillis(1L);
        Instant nowPlus2 = now.plusMillis(2L);

        Complex c1 = new Complex(new Complex.Id(999_999, now.toEpochMilli(), "AAA", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, nowPlus1.toEpochMilli(), "BBB", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_000, nowPlus2.toEpochMilli(), "CCC", Complex.Status.FAIL));
        db.tx(() -> db.complexes().insert(c1, c2, c3));

        db.tx(() -> {
            List<Complex> page = complexQuery()
                    .limit(100)
                    .filter(fb -> fb.where("id.a").in(999_999, 999_000).and("id.b").in(now, nowPlus2))
                    .orderBy(ob -> ob.orderBy("id.a").descending())
                    .find();
            Assertions.assertThat(page).containsExactly(c1, c3);
        });
    }

    @Test
    public void or() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput));

        db.tx(() -> {
            List<Project> page = projectQuery()
                    .where("id").eq("uuid002")
                    .or("id").eq("uuid777")
                    .limit(100)
                    .find();
            Assertions.assertThat(page).containsExactly(p1, p2);
        });
    }

    @Test
    public void notOr() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            List<Project> page = projectQuery()
                    .limit(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").eq("uuid002").or("id").eq("uuid777")
                            .build()))
                    .find();
            Assertions.assertThat(page).containsExactly(inOutput);
        });
    }

    @Test
    public void notRel() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            List<Project> page = projectQuery()
                    .limit(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").gt("uuid002")
                            .build()))
                    .find();
            Assertions.assertThat(page).containsExactly(p1);
        });
    }

    @Test
    public void notIn() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            List<Project> page = projectQuery()
                    .limit(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").in("uuid002", "uuid777")
                            .build()))
                    .find();
            Assertions.assertThat(page).containsExactly(inOutput);
        });
    }

    @Test
    public void listByNamesWithUnderscores() {
        TypeFreak tf = new TypeFreak(
                new TypeFreak.Id("first", 42),
                false,
                (byte) 0,
                (byte) 0,
                (short) 0,
                0,
                0L,
                0.0f,
                0.0,
                true,
                (byte) 0xF0,
                (byte) 0xFF,
                (short) 0xFFAF,
                100_500,
                1000000000000L,
                0.5f,
                0.25,
                "utf8",
                "str",
                new byte[0],
                Status.DRAFT,
                Status.OK,
                null,
                null,
                null,
                null,
                null,
                Instant.now().truncatedTo(MILLIS),
                emptyList(),
                emptyList(),
                emptySet(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                "CUSTOM NAMED COLUMN",
                null
        );
        db.tx(() -> db.typeFreaks().insert(tf));

        db.tx(() -> {
            List<TypeFreak> page = typeFreakQuery()
                    .limit(50)
                    .where("customNamedColumn").eq("CUSTOM NAMED COLUMN")
                    .find();
            Assertions.assertThat(page).containsExactly(tf);
            assertThat(page.size() < 50).isTrue();
        });
    }

    protected <T extends Entity<T>> TableQueryBuilder<T> getQueryBuilder(@NonNull Table<T> table) {
        return new TableQueryBuilder<>(table);
    }

    protected final TableQueryBuilder<Project> projectQuery() {
        return createQueryBuilder(Project.class);
    }

    protected final TableQueryBuilder<Complex> complexQuery() {
        return createQueryBuilder(Complex.class);
    }

    protected final TableQueryBuilder<TypeFreak> typeFreakQuery() {
        return createQueryBuilder(TypeFreak.class);
    }
}
