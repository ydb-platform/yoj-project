package tech.ydb.yoj.repository.db.table;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;

import java.util.stream.Stream;

public interface TableReader<T extends Entity<T>> {
    <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params);

    <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params);

    <V extends Table.ViewId<T>, ID extends Entity.Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params);

    default Stream<T> readTable() {
        return readTable(ReadTableParams.getDefault());
    }

    default <ID extends Entity.Id<T>> Stream<ID> readTableIds() {
        return readTableIds(ReadTableParams.getDefault());
    }
}
