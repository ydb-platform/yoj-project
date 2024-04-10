package tech.ydb.yoj.databind.converter;

import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

/**
 * Custom conversion logic between database column values ({@code <C>}) and Java field values ({@code <J>}).
 * <p>A good citizen {@code ValueConverter} must:
 * <ul>
 * <li>Have a no-args public constructor that does not perform any CPU- or I/O-intensive operations.</li>
 * <li>Be thread-safe and reentrant. {@code ValueConverter} might be created and called from any thread:
 * it is possible for {@code toColumn()} method to be called while the same instance's {@code toJava()}
 * method is running in a different thread, and vice versa. It it therefore <strong>highly</strong>
 * recommended for the conversion to be a <em>pure function</em>.</li>
 * <li>Be effectively stateless: changes to the internal state of the {@code ValueConverter} must not affect
 * the result of the conversions.</li>
 * <li>Never acquire scarce or heavy system resources, because YOJ might create a {@code ValueConverter}
 * at any time, and there is no way to dispose of a created {@code ValueConverter}.</li>
 * </ul>
 *
 * @param <J> Java field value type
 * @param <C> Database column value type. <strong>Must not</strong> be the same type as {@code <J>}. Must be {@link Comparable}.
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public interface ValueConverter<J, C extends Comparable<? super C>> {
    /**
     * Converts a field value to a {@link tech.ydb.yoj.databind.FieldValueType database column value} supported by YOJ.
     *
     * @param field schema field
     * @param v field value, guaranteed to not be {@code null}
     * @return database column value corresponding to the Java field value, must not be {@code null}
     *
     * @see #toJava(JavaField, Comparable)
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
    final class NoConverter implements ValueConverter<Void, Boolean> {
        private NoConverter() {
            throw new UnsupportedOperationException("Not instantiable");
        }

        @Override
        public @NonNull Boolean toColumn(@NonNull JavaField field, @NonNull Void v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NonNull Void toJava(@NonNull JavaField field, @NonNull Boolean unused) {
            throw new UnsupportedOperationException();
        }
    }
}
