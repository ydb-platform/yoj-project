package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RecordEntityTest {
    private record Ent(@NonNull Id id, int payload) implements RecordEntity<Ent> {
        private record Id(String part1, String parts) implements Entity.Id<RecordEntityTest.Ent> {
        }
    }

    @Test
    public void testPartialId() {
        var schema = EntitySchema.of(Ent.class);
        var completeId = new Ent.Id("a", "b");
        var partialId = new Ent.Id("a", null);

        assertTrue(TableQueryImpl.isPartialId(partialId, schema));
        assertFalse(TableQueryImpl.isPartialId(completeId, schema));
    }
}
