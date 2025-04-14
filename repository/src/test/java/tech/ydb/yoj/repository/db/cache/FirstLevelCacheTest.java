package tech.ydb.yoj.repository.db.cache;

import lombok.Value;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class FirstLevelCacheTest {
    private FirstLevelCache cache;
    private TableDescriptor<FooEntity> fooTableDescriptor;
    private TableDescriptor<BarEntity> barTableDescriptor;

    @Before
    public void setUp() {
        cache = FirstLevelCache.create();
        fooTableDescriptor = TableDescriptor.from(EntitySchema.of(FooEntity.class));
        barTableDescriptor = TableDescriptor.from(EntitySchema.of(BarEntity.class));
    }

    @Test
    public void testGetLoad() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        var actual = cache.get(fooTableDescriptor, id, __ -> entity);

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void testGetLoadNotFound() {
        var id = FooEntity.Id.of(17);
        boolean[] loaderCalled = {false};

        var actual = cache.get(fooTableDescriptor, id, __ -> {
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

        cache.get(fooTableDescriptor, id, __ -> entity);

        // Act
        var actual = cache.get(fooTableDescriptor, id, __ -> {
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
        cache.put(fooTableDescriptor, entity);

        // Verify
        var actual = cache.get(fooTableDescriptor, id, __ -> {
            Assertions.fail("Loader MUST NOT be called");
            return null;
        });

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void testPutEmpty() {
        var id = FooEntity.Id.of(17);
        var entity = new FooEntity(id);

        cache.put(fooTableDescriptor, entity);

        // Act
        cache.putEmpty(fooTableDescriptor, id);

        // Verify
        boolean[] loaderCalled = {false};
        var actual = cache.get(fooTableDescriptor, id, __ -> {
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

        cache.put(fooTableDescriptor, entity1);
        cache.put(fooTableDescriptor, entity2);
        cache.put(barTableDescriptor, entity3);
        cache.putEmpty(fooTableDescriptor, id);

        var snapshot = cache.snapshot(fooTableDescriptor);
        assertThat(snapshot).containsOnly(entity1, entity2);

        // FirstLevelCache.snapshot() is an immutable copy, so it should not see further changes to the cache
        var entity4 = new FooEntity(FooEntity.Id.of(99));
        cache.put(fooTableDescriptor, entity4);
        assertThat(snapshot).containsOnly(entity1, entity2);

        // Later snapshots should see the updated cache state
        var snapshot2 = cache.snapshot(fooTableDescriptor);
        assertThat(snapshot2).containsOnly(entity1, entity2, entity4);
    }

    @Test
    public void testEntities() {
        var entity1 = new FooEntity(FooEntity.Id.of(17));
        var entity2 = new FooEntity(FooEntity.Id.of(23));
        var entity3 = new BarEntity(BarEntity.Id.of("42"));
        var id = FooEntity.Id.of(42);

        cache.put(fooTableDescriptor, entity1);
        cache.put(fooTableDescriptor, entity2);
        cache.put(barTableDescriptor, entity3);
        cache.putEmpty(fooTableDescriptor, id);

        var entities = cache.entities(fooTableDescriptor);

        assertThat(entities).containsOnlyKeys(entity1.getId(), entity2.getId());
        assertThat(entities.get(entity1.getId())).isSameAs(entity1);
        assertThat(entities.get(entity2.getId())).isSameAs(entity2);
        assertThat(entities.get(id)).isNull();

        // FirstLevelCache.entities() is a view, so it should see further changes to the cache
        var entity4 = new FooEntity(FooEntity.Id.of(99));
        cache.put(fooTableDescriptor, entity4);
        assertThat(entities.get(entity4.getId())).isSameAs(entity4);
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
