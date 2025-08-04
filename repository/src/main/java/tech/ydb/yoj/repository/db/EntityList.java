package tech.ydb.yoj.repository.db;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
 * @deprecated Projections will be removed from the core YOJ API in 3.0.0 and possibly reintroduced as an optional module.
 */
@Deprecated(forRemoval = true)
public class EntityList extends LinkedList<Entity<?>> {
    public static EntityList create() {
        return new EntityList();
    }

    public EntityList having(boolean condition, Supplier<Entity<?>> supplier) {
        if (condition) {
            add(supplier.get());
        }
        return this;
    }

    public EntityList having(boolean condition, Entity<?> entity) {
        return having(condition, () -> entity);
    }

    public EntityList having(boolean condition, Stream<Entity<?>> entities) {
        if (condition) {
            entities.forEach(this::add);
        }
        return this;
    }

    public final EntityList with(Entity<?>... entities) {
        return with(List.of(entities));
    }

    public final EntityList with(Iterable<Entity<?>> entities) {
        entities.forEach(this::add);
        return this;
    }
}
