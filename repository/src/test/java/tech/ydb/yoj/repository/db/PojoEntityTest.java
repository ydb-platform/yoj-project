package tech.ydb.yoj.repository.db;

import lombok.NonNull;
import lombok.Value;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void testPartialId() {
        var completeId = new Ent.Id("a", "b");
        var partialId = new Ent.Id("a", null);

        assertTrue(partialId.isPartial());
        assertFalse(completeId.isPartial());
    }
}
