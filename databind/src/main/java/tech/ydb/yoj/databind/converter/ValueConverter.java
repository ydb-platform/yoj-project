package tech.ydb.yoj.databind.converter;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

/**
 * Custom conversion logic between database column values and Java field values.
 * <br><strong>Must</strong> have a no-args public constructor.
 *
 * @param <J> Java value type
 * @param <C> Database column value type
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public interface ValueConverter<J, C> {
    @NonNull
    C toColumn(@NonNull JavaField field, @NonNull J v);

    @NonNull
    J toJava(@NonNull JavaField field, @NonNull C c);

    class NoConverter implements ValueConverter<Void, Void> {
        private NoConverter() {
            throw new UnsupportedOperationException("Not instantiable");
        }

        @Override
        public @NonNull Void toColumn(@NonNull JavaField field, @NonNull Void v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NonNull Void toJava(@NonNull JavaField field, @NonNull Void unused) {
            throw new UnsupportedOperationException();
        }
    }
}
