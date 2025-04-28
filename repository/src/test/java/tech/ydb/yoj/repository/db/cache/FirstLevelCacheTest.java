package tech.ydb.yoj.repository.db.cache;

import lombok.Value;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;

import static org.assertj.core.api.Assertions.assertThat;

public class FirstLevelCacheTest {
    private FirstLevelCache cache;

    @Before
    public void setUp() {
        cache = new FirstLevelCacheImpl();
    }

    @Test
    public void testGetLoad() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        var actual = cache.get(id, __ -> entity);

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void testGetLoadNotFound() {
        var id = FooEntity.Id.of(17);
        boolean[] loaderCalled = {false};

        var actual = cache.get(id, __ -> {
            loaderCalled[0] = true;
            return null;
        });

        assertThat(loaderCalled[0]).isTrue();
        assertThat(actual).isNull();
    }

    @Test
    public void testGetFromCache() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        cache.get(id, __ -> entity);

        // Act
        var actual = cache.get(id, __ -> {
            Assertions.fail("Loader MUST NOT be called");
            return null;
        });

        // Verify
        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void testPut() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        // Act
        cache.put(entity);

        // Verify
        var actual = cache.get(id, __ -> {
            Assertions.fail("Loader MUST NOT be called");
            return null;
        });

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void testPutEmpty() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        cache.put(entity);

        // Act
        cache.putEmpty(id);

        // Verify
        boolean[] loaderCalled = {false};
        var actual = cache.get(id, __ -> {
            loaderCalled[0] = true;
            return null;
        });

        assertThat(loaderCalled[0]).isFalse();
        assertThat(actual).isNull();
    }

    @Test
    public void testSnapshot() {
        var entity1 = new FooEntity(FooEntity.Id.of(17));
        var entity2 = new FooEntity(FooEntity.Id.of(23));
        var entity3 = new BarEntity(BarEntity.Id.of("42"));
        var id = FooEntity.Id.of(42);

        cache.put(entity1);
        cache.put(entity2);
        cache.put(entity3);
        cache.putEmpty(id);

        // Act
        var snapshot = cache.snapshot(FooEntity.class);

        // Verify
        assertThat(snapshot).containsOnly(entity1, entity2);
    }

    @Value
    static class FooEntity implements Entity<FooEntity> {
        Id id;

        @Value(staticConstructor = "of")
        static class Id implements Entity.Id<FooEntity> {
            int value;
        }
    }

    @Value
    static class BarEntity implements Entity<BarEntity> {
        Id id;

        @Value(staticConstructor = "of")
        static class Id implements Entity.Id<BarEntity> {
            String value;
        }
    }
}
