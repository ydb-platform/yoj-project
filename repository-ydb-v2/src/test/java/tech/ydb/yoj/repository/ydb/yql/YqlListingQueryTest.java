package tech.ydb.yoj.repository.ydb.yql;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

public class YqlListingQueryTest {
    private static final Schema<ComplexObj> complexSchema = EntitySchema.of(ComplexObj.class);

    @Test
    public void sortSubexpressions() {
        FilterExpression<ComplexObj> filter = FilterBuilder.forSchema(complexSchema)
                .where("val1").gt(5)
                .and("id.timestamp").gte(100_500L)
                .and("id.key").eq("uzhos")
                .and("val2").neq("yozhos")
                .build();

        assertThat(filter).hasToString("(val1 > 5) AND (id.timestamp >= 100500) AND (id.key == \"uzhos\") AND (val2 != \"yozhos\")");
        assertThat(YqlListingQuery.normalize(filter)).hasToString("(id.key == \"uzhos\") AND (id.timestamp >= 100500) AND (val1 > 5) AND (val2 != \"yozhos\")");
    }

    @Test
    public void likePatternEscapesEscapeChar() {
        assertThat(toYqlPredicate(filterBuilder -> filterBuilder.where("val2").contains("a^b")))
                .hasToString("val2 LIKE %a^^b% ESCAPE ^");
        assertThat(toYqlPredicate(filterBuilder -> filterBuilder.where("val2").startsWith("a^")))
                .hasToString("val2 LIKE a^^% ESCAPE ^");
        assertThat(toYqlPredicate(filterBuilder -> filterBuilder.where("val2").endsWith("^b")))
                .hasToString("val2 LIKE %^^b ESCAPE ^");
    }

    @Test
    public void likePatternEscapesWildcardsAndEscapeChar() {
        assertThat(toYqlPredicate(filterBuilder -> filterBuilder.where("val2").contains("%a^_")))
                .hasToString("val2 LIKE %^%a^^^_% ESCAPE ^");
    }

    @Test
    public void likePatternWithoutSpecialChars() {
        assertThat(toYqlPredicate(filterBuilder -> filterBuilder.where("val2").contains("abc")))
                .hasToString("val2 LIKE %abc% ESCAPE ^");
    }

    private static YqlPredicate toYqlPredicate(UnaryOperator<FilterBuilder<ComplexObj>> filterCtor) {
        return YqlListingQuery.toYqlPredicate(filterCtor.apply(FilterBuilder.forSchema(complexSchema)).build());
    }

    @Value
    private static class ComplexObj implements Entity<ComplexObj> {
        Id id;

        int val1;
        String val2;

        @Value
        private static class Id implements Entity.Id<ComplexObj> {
            String key;
            long timestamp;
        }
    }
}
