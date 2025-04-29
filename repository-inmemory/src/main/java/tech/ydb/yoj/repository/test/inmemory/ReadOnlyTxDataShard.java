package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import javax.annotation.Nullable;
import java.util.List;

/*package*/ interface ReadOnlyTxDataShard<T extends Entity<T>> {
    @Nullable
    T find(Entity.Id<T> id);

    @Nullable
    <V extends Table.View> V find(Entity.Id<T> id, Class<V> viewType);

    List<T> findAll();
}
