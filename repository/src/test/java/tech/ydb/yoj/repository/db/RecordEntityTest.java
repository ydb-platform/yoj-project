package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordEntityTest {
    private record Ent(@NonNull Id id, int payload) implements RecordEntity<Ent> {
        private record Id(String part1, String parts) implements Entity.Id<RecordEntityTest.Ent> {
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
