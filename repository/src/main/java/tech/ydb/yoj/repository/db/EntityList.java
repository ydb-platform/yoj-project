package tech.ydb.yoj.repository.db;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
