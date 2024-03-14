package tech.ydb.yoj.databind.converter;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

/**
 * Custom conversion logic between database column values and Java field values.
 * <br><strong>Must</strong> have a no-args public constructor.
 *
 * @param <J> Java field value type
 * @param <C> Database column value type. <strong>Must not</strong> be the same type as {@code <J>}.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public interface ValueConverter<J, C> {
    /**
     * Converts a field value to a {@link tech.ydb.yoj.databind.FieldValueType database column value} supported by YOJ.
     *
     * @param field schema field
     * @param v field value, guaranteed to not be {@code null}
     * @return database column value corresponding to the Java field value, must not be {@code null}
     *
     * @see #toJava(JavaField, Object)
     */
    @NonNull
    C toColumn(@NonNull JavaField field, @NonNull J v);

    /**
     * Converts a database column value to a Java field value.
     *
     * @param field schema field
     * @param c database column value, guaranteed to not be {@code null}
     * @return Java field value corresponding to the database column value, must not be {@code null}
     *
     * @see #toColumn(JavaField, Object)
     */
    @NonNull
    J toJava(@NonNull JavaField field, @NonNull C c);

    /**
     * Represents "no custom converter is defined" for {@link tech.ydb.yoj.databind.CustomValueType @CustomValueType}
     * annotation inside a {@link tech.ydb.yoj.databind.schema.Column @Column} annotation.
     * <p>Non-instantiable, every method including the constructor throws {@link UnsupportedOperationException}.
     */
    final class NoConverter implements ValueConverter<Void, Void> {
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
