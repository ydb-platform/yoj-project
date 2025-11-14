package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import lombok.Value;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PojoEntityTest {
    @Value
    private static class Ent implements Entity<Ent> {
        @NonNull
        Id id;
        int payload;

        @Value
        private static class Id implements Entity.Id<Ent> {
            String part1;
            String part2;
        }
    }

    @Test
    public void partialId() {
        var completeId = new Ent.Id("a", "b");
        var partialId = new Ent.Id("a", null);

        assertThat(partialId.isPartial()).withFailMessage("expected ('a', null) to be a partial ID").isTrue();
        assertThat(completeId.isPartial()).withFailMessage("expected ('a', b') to be a non-partial ID").isFalse();
    }
}
