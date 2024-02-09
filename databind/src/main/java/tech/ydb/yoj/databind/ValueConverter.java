package tech.ydb.yoj.databind;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;

/**
 * Custom value conversion logic. Must have a no-args public constructor.
 *
 * @param <V> Java value type
 * @param <C> Database column value type
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public interface ValueConverter<V, C> {
    @NonNull
    C toColumn(@NonNull V v);

    @NonNull
    V toJava(@NonNull C c);
}
