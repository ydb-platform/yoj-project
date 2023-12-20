package tech.ydb.yoj.databind.expression;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;


public class FilterExpressionTest {
    private static final Schema<Obj> schema = ObjectSchema.of(Obj.class);

    @Test
    public void andChain() {
        FilterExpression<Obj> userFilter = FilterBuilder.forSchema(schema)
                .where("x").gt(5)
                .and("y").lt(7)
                .build();
        assertThat(userFilter.toString()).isEqualTo("(x > 5) AND (y < 7)");

        FilterExpression<Obj> fullFilter = FilterBuilder.forSchema(schema)
                .where("id").eq("Hello, world!")
                .and(userFilter)
                .build();
        assertThat(fullFilter.toString()).isEqualTo("(id == \"Hello, world!\") AND (x > 5) AND (y < 7)");
    }

    @Test
    public void orChain() {
        FilterExpression<Obj> f0 = FilterBuilder.forSchema(schema)
                .where("x").gt(5)
                .or("y").lt(7)
                .build();
        assertThat(f0.toString()).isEqualTo("(x > 5) OR (y < 7)");

        FilterExpression<Obj> f1 = FilterBuilder.forSchema(schema)
                .where("id").eq("Hello, world!")
                .or(f0)
                .build();
        assertThat(f1.toString()).isEqualTo("(id == \"Hello, world!\") OR (x > 5) OR (y < 7)");
    }

    @Test
    public void orAndOr() {
        FilterExpression<Obj> or1 = FilterBuilder.forSchema(schema)
                .where("x").eq(500)
                .or("y").lt(30)
                .build();
        assertThat(or1.toString()).isEqualTo("(x == 500) OR (y < 30)");

        FilterExpression<Obj> or2 = FilterBuilder.forSchema(schema)
                .where("y").eq(29)
                .or("x").lt(1000)
                .build();
        assertThat(or2.toString()).isEqualTo("(y == 29) OR (x < 1000)");

        FilterExpression<Obj> combined = or1.and(or2);
        assertThat(combined.toString()).isEqualTo("((x == 500) OR (y < 30)) AND ((y == 29) OR (x < 1000))");
    }

    @Test
    public void andOrAnd() {
        FilterExpression<Obj> and1 = FilterBuilder.forSchema(schema)
                .where("x").eq(500)
                .and("y").lt(30)
                .build();
        assertThat(and1.toString()).isEqualTo("(x == 500) AND (y < 30)");

        FilterExpression<Obj> and2 = FilterBuilder.forSchema(schema)
                .where("y").eq(29)
                .and("x").lt(1000)
                .build();
        assertThat(and2.toString()).isEqualTo("(y == 29) AND (x < 1000)");

        FilterExpression<Obj> combined = and1.or(and2);
        assertThat(combined.toString()).isEqualTo("((x == 500) AND (y < 30)) OR ((y == 29) AND (x < 1000))");
    }

    @Test
    public void orOrLeaf() {
        FilterExpression<Obj> or = FilterBuilder.forSchema(schema)
                .where("x").eq(500)
                .or("y").lt(30)
                .build();
        assertThat(or.toString()).isEqualTo("(x == 500) OR (y < 30)");

        FilterExpression<Obj> leaf = FilterBuilder.forSchema(schema)
                .where("y").eq(29)
                .build();
        assertThat(leaf.toString()).isEqualTo("y == 29");

        FilterExpression<Obj> combined = or.or(leaf);
        assertThat(combined.toString()).isEqualTo("(x == 500) OR (y < 30) OR (y == 29)");
    }

    @Test
    public void andAndLeaf() {
        FilterExpression<Obj> and = FilterBuilder.forSchema(schema)
                .where("x").eq(500)
                .and("y").lt(30)
                .build();
        assertThat(and.toString()).isEqualTo("(x == 500) AND (y < 30)");

        FilterExpression<Obj> leaf = FilterBuilder.forSchema(schema)
                .where("y").eq(29)
                .build();
        assertThat(leaf.toString()).isEqualTo("y == 29");

        FilterExpression<Obj> combined = and.and(leaf);
        assertThat(combined.toString()).isEqualTo("(x == 500) AND (y < 30) AND (y == 29)");
    }

    @Test
    public void complexChain1() {
        FilterExpression<Obj> orF0 = FilterBuilder.forSchema(schema)
                .where("x").eq(1337)
                .build();
        assertThat(orF0.toString()).isEqualTo("x == 1337");

        FilterExpression<Obj> orF1 = FilterBuilder.forSchema(schema)
                .where("x").gt(5)
                .or("y").lt(7)
                .or(orF0)
                .build();
        assertThat(orF1.toString()).isEqualTo("(x > 5) OR (y < 7) OR (x == 1337)");

        FilterExpression<Obj> andF0 = FilterBuilder.forSchema(schema)
                .where("y").gte(0)
                .and(orF1)
                .build();
        assertThat(andF0.toString()).isEqualTo("(y >= 0) AND ((x > 5) OR (y < 7) OR (x == 1337))");

        FilterExpression<Obj> andF1 = FilterBuilder.forSchema(schema)
                .where("id").eq("Hello, world!")
                .and(andF0)
                .build();
        assertThat(andF1.toString()).isEqualTo("(id == \"Hello, world!\") AND (y >= 0) AND ((x > 5) OR (y < 7) OR (x == 1337))");
    }

    @Test
    public void complexChain2() {
        FilterExpression<Obj> orF0 = FilterBuilder.forSchema(schema)
                .where("x").eq(1337)
                .or("y").between(35, 97)
                .build();
        assertThat(orF0.toString()).isEqualTo("(x == 1337) OR ((y >= 35) AND (y <= 97))");

        FilterExpression<Obj> orF1 = FilterBuilder.forSchema(schema)
                .where("x").gt(5)
                .or("y").lt(7)
                .or(orF0)
                .build();
        assertThat(orF1.toString()).isEqualTo("(x > 5) OR (y < 7) OR (x == 1337) OR ((y >= 35) AND (y <= 97))");

        FilterExpression<Obj> andF0 = FilterBuilder.forSchema(schema)
                .where("y").gte(0)
                .and(orF1)
                .build();
        assertThat(andF0.toString()).isEqualTo("(y >= 0) AND ((x > 5) OR (y < 7) OR (x == 1337) OR ((y >= 35) AND (y <= 97)))");

        FilterExpression<Obj> andF1 = FilterBuilder.forSchema(schema)
                .where("id").eq("Hello, world!")
                .and(andF0)
                .build();
        assertThat(andF1.toString()).isEqualTo("(id == \"Hello, world!\") AND (y >= 0) AND ((x > 5) OR (y < 7) OR (x == 1337) OR ((y >= 35) AND (y <= 97)))");
    }

    @Test
    public void embeddedIsNull() {
        FilterExpression<Obj> f = FilterBuilder.forSchema(schema)
                .where("embedded").isNull()
                .build();
        assertThat(f.toString()).isEqualTo("embedded IS NULL");
    }

    @Test
    public void embeddedIsNotNull() {
        FilterExpression<Obj> f = FilterBuilder.forSchema(schema)
                .where("embedded").isNotNull()
                .build();
        assertThat(f.toString()).isEqualTo("embedded IS NOT NULL");
    }

    @Value
    private static class Obj {
        String id;

        int x;
        int y;

        @Column(flatten = false)
        EmbeddedObj embedded;
    }

    @Value
    private static class EmbeddedObj {
        String s;
    }
}
