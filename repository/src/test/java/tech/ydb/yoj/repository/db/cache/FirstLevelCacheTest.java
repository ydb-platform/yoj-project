package tech.ydb.yoj.repository.db.cache;

import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.RecordEntity;
import tech.ydb.yoj.repository.db.TableDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FirstLevelCacheTest {
    private FirstLevelCacheProvider cacheProvider;
    private TableDescriptor<FooEntity> fooTableDescriptor;
    private TableDescriptor<BarEntity> barTableDescriptor;

    @Before
    public void setUp() {
        cacheProvider = new FirstLevelCacheProvider(FirstLevelCache::create);
        fooTableDescriptor = TableDescriptor.from(EntitySchema.of(FooEntity.class));
        barTableDescriptor = TableDescriptor.from(EntitySchema.of(BarEntity.class));
    }

    @Test
    public void getLoad() {
        var id = new FooEntity.Id(17);
        var entity = new FooEntity(id);

        var cache = cacheProvider.getOrCreate(fooTableDescriptor);
        var actual = cache.get(id, __ -> entity);

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void getLoadNotFound() {
        var id = new FooEntity.Id(17);
        boolean[] loaderCalled = {false};

        var cache = cacheProvider.getOrCreate(fooTableDescriptor);
        var actual = cache.get(id, __ -> {
            loaderCalled[0] = true;
            return null;
        });

        assertThat(loaderCalled[0]).isTrue();
        assertThat(actual).isNull();
    }

    @Test
    public void getFromCache() {
        var id = new FooEntity.Id(17);
        var entity = new FooEntity(id);

        var cache = cacheProvider.getOrCreate(fooTableDescriptor);
        cache.get(id, __ -> entity);

        // Act
        var actual = cache.get(id, __ -> {
            fail("Loader MUST NOT be called");
            return null;
        });

        // Verify
        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void put() {
        var id = new FooEntity.Id(17);
        var entity = new FooEntity(id);

        // Act
        var cache = cacheProvider.getOrCreate(fooTableDescriptor);
        cache.put(entity);

        // Verify
        var actual = cache.get(id, __ -> {
            fail("Loader MUST NOT be called");
            return null;
        });

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void putEmpty() {
        var id = new FooEntity.Id(17);
        var entity = new FooEntity(id);

        // Act
        var cache = cacheProvider.getOrCreate(fooTableDescriptor);
        cache.put(entity);

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
    public void snapshot() {
        var entity1 = new FooEntity(new FooEntity.Id(17));
        var entity2 = new FooEntity(new FooEntity.Id(23));
        var entity3 = new BarEntity(new BarEntity.Id("42"));
        var id = new FooEntity.Id(42);

        var fooCache = cacheProvider.getOrCreate(fooTableDescriptor);
        var barCache = cacheProvider.getOrCreate(barTableDescriptor);

        fooCache.put(entity1);
        fooCache.put(entity2);
        barCache.put(entity3);
        fooCache.putEmpty(id);

        var fooSnapshot = fooCache.snapshot();
        var barSnapshot = barCache.snapshot();
        assertThat(fooSnapshot).containsOnly(entity1, entity2);
        assertThat(barSnapshot).containsOnly(entity3);

        // FirstLevelCache.snapshot() is an immutable copy, so it should not see further changes to the cache
        var entity4 = new FooEntity(new FooEntity.Id(99));
        fooCache.put(entity4);
        assertThat(fooSnapshot).containsOnly(entity1, entity2);

        // Later snapshots should see the updated cache state
        var fooSnapshot2 = fooCache.snapshot();
        assertThat(fooSnapshot2).containsOnly(entity1, entity2, entity4);
    }

    record FooEntity(FooEntity.Id id) implements RecordEntity<FooEntity> {
        record Id(int value) implements Entity.Id<FooEntity> {
        }
    }

    record BarEntity(BarEntity.Id id) implements RecordEntity<BarEntity> {
        record Id(String value) implements Entity.Id<BarEntity> {
        }
    }
}
