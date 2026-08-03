package tech.ydb.yoj.repository.db;

import org.junit.Test;
import tech.ydb.yoj.databind.expression.OrderExpression.SortKey;
import tech.ydb.yoj.databind.schema.GlobalIndex;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.databind.expression.OrderExpression.SortOrder.ASCENDING;

public class EntityExpressionsTest {
    @Test
    public void orderByIndex() {
        record OtherEntity(Id id) implements RecordEntity<OtherEntity> {
            record Id(String value) implements RecordEntity.Id<OtherEntity> {
            }
        }

        @GlobalIndex(name = "other_index", fields = {"id.otherId", "id.typename"})
        record IndexedEntity(Id id) implements RecordEntity<IndexedEntity> {
            record Id(String typename, OtherEntity.Id otherId) implements RecordEntity.Id<IndexedEntity> {
            }
        }

        var schema = EntitySchema.of(IndexedEntity.class);
        var orderBy = EntityExpressions.orderByIndex(schema, "other_index", IndexOrder.ASCENDING);
        assertThat(orderBy.getKeys()).containsExactly(
                new SortKey(schema.getField("id.otherId"), ASCENDING),
                new SortKey(schema.getField("id.typename"), ASCENDING)
        );
    }
}
