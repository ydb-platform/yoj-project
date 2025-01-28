package tech.ydb.yoj.repository.ydb.yql;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.List;

public interface YqlStatementPart<P extends YqlStatementPart<P>> {
    String getType();

    int getPriority();

    default <T extends Entity<T>> String toFullYql(@NonNull EntitySchema<T> schema) {
        return getYqlPrefix() + toYql(schema);
    }

    String getYqlPrefix();

    <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema);

    default List<? extends YqlStatementPart<?>> combine(@NonNull List<? extends P> other) {
        throw new UnsupportedOperationException("Multiple " + getType() + " specifications are not supported");
    }
}
