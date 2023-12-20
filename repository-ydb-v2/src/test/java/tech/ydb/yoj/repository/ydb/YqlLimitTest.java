package tech.ydb.yoj.repository.ydb;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlLimit;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class YqlLimitTest {
    private final EntitySchema<EmptyEntity> emptySchema = EntitySchema.of(EmptyEntity.class);

    @Test
    public void nonempty_top() {
        YqlLimit lim = YqlLimit.top(100L);

        assertThat(lim.size()).isEqualTo(100L);
        assertThat(lim.isEmpty()).isFalse();
        assertThat(lim.toYql(emptySchema)).isEqualToIgnoringCase("100");
    }

    @Test
    public void empty_top() {
        assertThat(YqlLimit.top(0L)).isEqualTo(YqlLimit.EMPTY);
    }

    @Test
    public void empty_range_without_offset() {
        YqlLimit lim = YqlLimit.range(0, 0);

        assertThat(lim.size()).isEqualTo(0L);
        assertThat(lim.isEmpty()).isTrue();
        assertThat(lim.toYql(emptySchema)).isEqualTo("0");
    }

    @Test
    public void nonempty_range_without_offset() {
        YqlLimit lim = YqlLimit.range(0, 27);

        assertThat(lim.size()).isEqualTo(27L);
        assertThat(lim.isEmpty()).isFalse();
        assertThat(lim.toYql(emptySchema)).isEqualTo("27");
    }

    @Test
    public void empty_range_with_offset() {
        YqlLimit lim = YqlLimit.range(5, 5);

        assertThat(lim.size()).isEqualTo(0L);
        assertThat(lim.isEmpty()).isTrue();
        assertThat(lim.toYql(emptySchema)).isEqualTo("0");
    }

    @Test
    public void nonempty_range_with_offset() {
        YqlLimit lim = YqlLimit.range(5, 7);

        assertThat(lim.size()).isEqualTo(2L);
        assertThat(lim.isEmpty()).isFalse();
        assertThat(lim.toYql(emptySchema)).isEqualTo("2 OFFSET 5");
    }

    @Test
    public void with_limit() {
        assertThat(YqlLimit.range(15, 30).withLimit(20)).isEqualTo(YqlLimit.range(15, 35));
    }

    @Test
    public void with_offset() {
        assertThat(YqlLimit.top(40).withOffset(5)).isEqualTo(YqlLimit.range(5, 45));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void multiple_limit_specs_are_not_supported() {
        YqlLimit lim = YqlLimit.top(100);
        lim.combine(singletonList(YqlLimit.range(0, 200)));
    }

    @Value
    private static final class EmptyEntity implements Entity<EmptyEntity> {
        private Id id;

        @Value
        private static final class Id implements Entity.Id<EmptyEntity> {
            int value2;
        }
    }
}
