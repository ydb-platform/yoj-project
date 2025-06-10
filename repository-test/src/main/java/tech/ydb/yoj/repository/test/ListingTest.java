package tech.ydb.yoj.repository.test;

import org.junit.Test;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.list.BadListingException.BadOffset;
import tech.ydb.yoj.repository.db.list.BadListingException.BadPageSize;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListRequest.ListingParams;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak.Status;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static tech.ydb.yoj.databind.expression.FilterBuilder.not;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;
import static tech.ydb.yoj.repository.db.EntityExpressions.newOrderBuilder;

public abstract class ListingTest extends RepositoryTestSupport {
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

    @Test
    public void basic() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        Project p3 = new Project(new Project.Id("uuid001"), "ZZZ");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput, p3));

        OrderExpression<Project> orderBy = newOrderBuilder(Project.class).orderBy("name").descending().build();
        FilterExpression<Project> filter = newFilterBuilder(Project.class).where("name").in("AAA", "XXX", "ZZZ").build();
        db.tx(() -> {
            ListResult<Project> page1 = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(1)
                    .orderBy(orderBy)
                    .filter(filter)
                    .build());
            assertThat(page1).containsExactly(p3);

            ListResult<Project> page2 = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(1)
                    .orderBy(orderBy)
                    .filter(filter)
                    .offset(1)
                    .build());
            assertThat(page2).containsExactly(p2);

            ListResult<Project> page3 = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(1)
                    .orderBy(orderBy)
                    .filter(filter)
                    .offset(2)
                    .build());
            assertThat(page3).containsExactly(p1);
            assertThat(page3.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(3)
                    .filter(fb -> fb.where("id.a").eq(999_999))
                    .build());
            assertThat(page).containsExactly(c3, c2, c1);
            assertThat(page.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(3)
                    .filter(fb -> fb.where("id.c").eq("UUU"))
                    .build());
            assertThat(page).containsExactly(c2);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void failOnZeroPageSize() {
        db.tx(() -> {
            assertThatExceptionOfType(BadPageSize.class).isThrownBy(() ->
                    db.projects().list(ListRequest.builder(Project.class)
                            .pageSize(0)
                            .build()));
        });
    }

    @Test
    public void failOnTooLargePageSize() {
        db.tx(() -> {
            assertThatExceptionOfType(BadPageSize.class).isThrownBy(() ->
                    db.projects().list(ListRequest.builder(Project.class)
                            .pageSize(100_000)
                            .build()));
        });
    }

    @Test
    public void failOnTooLargeOffset() {
        db.tx(() -> {
            assertThatExceptionOfType(BadOffset.class).isThrownBy(() ->
                    db.projects().list(ListRequest.builder(Project.class)
                            .offset(10_001)
                            .build()));
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(4)
                    .build());
            assertThat(page).containsExactly(c4, c3, c2, c1);
            assertThat(page.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(3)
                    .filter(fb -> fb.where("id.a").eq(1).and("id.b").gte(100L).and("id.b").lte(300L))
                    .build());
            assertThat(page).containsExactly(c1, c2, c3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void enumParsing() {
        OrderExpression<TypeFreak> orderBy = newOrderBuilder(TypeFreak.class).orderBy("status").descending().build();
        FilterExpression<TypeFreak> filter = newFilterBuilder(TypeFreak.class)
                .where("status").eq(Status.DRAFT)
                .build();
        ListRequest<TypeFreak> request = ListRequest.builder(TypeFreak.class)
                .pageSize(1)
                .orderBy(orderBy)
                .filter(filter)
                .build();

        db.tx(() -> db.typeFreaks()
                .list(request)
                .returnWithParams(ListingParams.empty()));
    }

    @Test
    public void embeddedNulls() {
        db.tx(() -> db.typeFreaks().insert(
                new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
        ));
        ListResult<TypeFreak> lst = db.tx(() -> db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                .filter(fb -> fb.where("embedded.a.a").eq("myfqdn"))
                .pageSize(1)
                .build()));

        assertThat(lst).isEmpty();
        assertThat(lst.isLastPage()).isTrue();
    }

    @Test
    public void flattenedIsNull() {
        var tf = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        db.tx(() -> db.typeFreaks().insert(tf));

        ListResult<TypeFreak> lst = db.tx(() -> db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                .filter(fb -> fb.where("jsonEmbedded").isNull())
                .pageSize(1)
                .build()));

        assertThat(lst).containsOnly(tf);
        assertThat(lst.isLastPage()).isTrue();
    }

    @Test
    public void flattenedIsNotNull() {
        var tf = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, new TypeFreak.Embedded(new TypeFreak.A("A"), new TypeFreak.B("B")), null, null, null, null, null, null, null, null, null, null, null);
        db.tx(() -> db.typeFreaks().insert(tf));

        ListResult<TypeFreak> lst = db.tx(() -> db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                .filter(fb -> fb.where("jsonEmbedded").isNotNull())
                .pageSize(1)
                .build()));

        assertThat(lst).containsOnly(tf);
        assertThat(lst.isLastPage()).isTrue();
    }

    @Test
    public void simpleIdIn() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        Project p3 = new Project(new Project.Id("uuid001"), "ZZZ");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput, p3));

        FilterExpression<Project> filter = newFilterBuilder(Project.class).where("id").in("uuid777", "uuid001", "uuid002").build();
        OrderExpression<Project> orderBy = newOrderBuilder(Project.class).orderBy("id").ascending().build();
        db.tx(() -> {
            ListResult<Project> page = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(100)
                    .filter(filter)
                    .orderBy(orderBy)
                    .build());
            assertThat(page).containsExactlyInAnyOrder(p1, p2, p3);
            assertThat(page.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(100)
                    .filter(fb -> fb
                            .where("id.a").in(999_999, 999_000)
                            .and("id.b").in(15L, 13L)
                            .and("id.c").in("AAA", "CCC")
                            .and("id.d").in(Complex.Status.OK, Complex.Status.FAIL)
                    )
                    .orderBy(ob -> ob.orderBy("id").descending())
                    .build());
            assertThat(page).containsExactly(c1, c3);
            assertThat(page.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("id.a").in(999_999, 999_000).and("id.b").gte(now).and("id.b").lt(nowPlus2))
                    .orderBy(ob -> ob.orderBy("id.a").descending())
                    .build());
            assertThat(page).containsExactlyInAnyOrder(c1, c2);
            assertThat(page.isLastPage()).isTrue();
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
            ListResult<Complex> page = db.complexes().list(ListRequest.builder(Complex.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("id.a").in(999_999, 999_000).and("id.b").in(now, nowPlus2))
                    .orderBy(ob -> ob.orderBy("id.a").descending())
                    .build());
            assertThat(page).containsExactly(c1, c3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void or() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project notInOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, notInOutput));

        db.tx(() -> {
            ListResult<Project> page = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(100)
                    .filter(newFilterBuilder(Project.class)
                            .where("id").eq("uuid002").or("id").eq("uuid777")
                            .build())
                    .build());
            assertThat(page).containsExactly(p1, p2);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void notOr() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            ListResult<Project> page = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").eq("uuid002").or("id").eq("uuid777")
                            .build()))
                    .build());
            assertThat(page).containsExactly(inOutput);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void notRel() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            ListResult<Project> page = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").gt("uuid002")
                            .build()))
                    .build());
            assertThat(page).containsExactly(p1);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void notIn() {
        Project p1 = new Project(new Project.Id("uuid002"), "AAA");
        Project inOutput = new Project(new Project.Id("uuid333"), "WWW");
        Project p2 = new Project(new Project.Id("uuid777"), "XXX");
        db.tx(() -> db.projects().insert(p1, p2, inOutput));

        db.tx(() -> {
            ListResult<Project> page = db.projects().list(ListRequest.builder(Project.class)
                    .pageSize(100)
                    .filter(not(newFilterBuilder(Project.class)
                            .where("id").in("uuid002", "uuid777")
                            .build()))
                    .build());
            assertThat(page).containsExactly(inOutput);
            assertThat(page.isLastPage()).isTrue();
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
                TypeFreak.Status.DRAFT,
                TypeFreak.Status.OK,
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
                null,
                "CUSTOM NAMED COLUMN",
                null
        );
        db.tx(() -> db.typeFreaks().insert(tf));

        db.tx(() -> {
            ListResult<TypeFreak> page = db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                    .pageSize(50)
                    .filter(newFilterBuilder(TypeFreak.class)
                            .where("customNamedColumn").eq("CUSTOM NAMED COLUMN")
                            .build())
                    .build());
            assertThat(page).containsExactly(tf);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void listStringValuedFilteredByString() {
        TypeFreak typeFreak = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, new TypeFreak.Ticket("CLOUD", 100500));
        db.tx(() -> db.typeFreaks().insert(typeFreak));

        db.tx(() -> {
            ListResult<TypeFreak> page = db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                    .pageSize(100)
                    .filter(newFilterBuilder(TypeFreak.class)
                            .where("ticket").eq("CLOUD-100500")
                            .build())
                    .build());
            assertThat(page).containsExactly(typeFreak);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void listStringValuedFilteredByString2() {
        TypeFreak typeFreak = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, new TypeFreak.StringValueWrapper("svw 123"), null, null);
        db.tx(() -> db.typeFreaks().insert(typeFreak));

        db.tx(() -> {
            ListResult<TypeFreak> page = db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                    .pageSize(100)
                    .filter(newFilterBuilder(TypeFreak.class)
                            .where("stringValueWrapper").eq("svw 123")
                            .build())
                    .build());
            assertThat(page).containsExactly(typeFreak);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void listStringValuedFilteredByStruct() {
        TypeFreak.Ticket ticket = new TypeFreak.Ticket("CLOUD", 100500);
        TypeFreak typeFreak = new TypeFreak(new TypeFreak.Id("b1p", 1), false, (byte) 0, (byte) 0, (short) 0, 0, 0, 0.0f, 0.0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, ticket);
        db.tx(() -> db.typeFreaks().insert(typeFreak));

        db.tx(() -> {
            ListResult<TypeFreak> page = db.typeFreaks().list(ListRequest.builder(TypeFreak.class)
                    .pageSize(100)
                    .filter(newFilterBuilder(TypeFreak.class)
                            .where("ticket").eq(ticket)
                            .build())
                    .build());
            assertThat(page).containsExactly(typeFreak);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void contains() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "earliest msg");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "middle msg");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "latest msg");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").contains("msg"))
                    .build());
            assertThat(page).containsExactly(e1, e2, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void iContains() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "earliest msg");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "middle msg");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "latest msg");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = listLogEntries(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").containsIgnoreCase("MsG"))
                    .build());
            assertThat(page).containsExactly(e1, e2, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void notContains() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "earliest msg");
        LogEntry inOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "middle msg");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "latest msg");
        db.tx(() -> db.logEntries().insert(e1, e2, inOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").doesNotContain("msg"))
                    .build());
            assertThat(page).containsExactly(inOutput);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void containsEscaped() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "%_acme-challenge.blahblahblah.");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "__hi%_there_");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "%_");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").contains("%_"))
                    .build());
            assertThat(page).containsExactly(e1, e2, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void startsWith() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "#tag earliest msg");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "#tag middle msg");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "#tag latest msg");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").startsWith("#tag"))
                    .build());
            assertThat(page).containsExactly(e1, e2, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void startsWithEscaped() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "%_acme-challenge.blahblahblah.");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "__hi%_there_");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "%_");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").startsWith("%_"))
                    .build());
            assertThat(page).containsExactly(e1, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void endsWith() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "earliest msg #tag");
        LogEntry inOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "middle msg #tag");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "latest msg #tag");
        db.tx(() -> db.logEntries().insert(e1, e2, inOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").endsWith(" #tag"))
                    .build());
            assertThat(page).containsExactly(e1, e2, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }

    @Test
    public void endsWithEscaped() {
        LogEntry e1 = new LogEntry(new LogEntry.Id("log1", 1L), LogEntry.Level.ERROR, "acme-challenge.blahblahblah.%_");
        LogEntry notInOutput = new LogEntry(new LogEntry.Id("log2", 2L), LogEntry.Level.DEBUG, "will be ignored");
        LogEntry e2 = new LogEntry(new LogEntry.Id("log1", 4L), LogEntry.Level.WARN, "__hi%_there_");
        LogEntry e3 = new LogEntry(new LogEntry.Id("log1", 5L), LogEntry.Level.INFO, "%_");
        db.tx(() -> db.logEntries().insert(e1, e2, notInOutput, e3));

        db.tx(() -> {
            ListResult<LogEntry> page = db.logEntries().list(ListRequest.builder(LogEntry.class)
                    .pageSize(100)
                    .filter(fb -> fb.where("message").endsWith("%_"))
                    .build());
            assertThat(page).containsExactly(e1, e3);
            assertThat(page.isLastPage()).isTrue();
        });
    }
}
