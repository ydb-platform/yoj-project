package tech.ydb.yoj.util.function;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Stream-processing methods should take a {@code Stream}-returning lambda, not a raw {@code Stream}.
 * {@code StreamSupplier} is such a lambda: its {@link #stream()} method is defined to return a fresh stream on every
 * call.
 * <p>
 * (<em>Motivation</em>: stream-processing methods almost surely will traverse the stream, and also might do a terminal
 * operation, e.g., {@code forEach}, {@code max()}, {@code collect()} etc.) or even {@code close()} the stream.
 * Subsequent calls to the same {@code Stream} might process only a part of the stream, or result in
 * {@code IllegalStateException}s.)
 * <p>
 * {@code StreamSupplier} has convenience {@code forEach[Ordered]()} methods for immediately traversing the stream
 * returned by {@link #stream()}, and can be used everywhere where a {@code Supplier} is expected (as it extends the
 * {@code Supplier} interface).
 *
 * @param <T> stream element type
 * @see #stream()
 * @see #forEach(Consumer)
 * @see #forEachOrdered(Consumer)
 */
@FunctionalInterface
public interface StreamSupplier<T> extends Supplier<Stream<T>> {
    /**
     * Returns a fresh stream, guaranteed to be open and not consumed by a terminal stream operation.
     * <p>
     * Whether the stream returned should be {@link Stream#close() closed} after use depends on the {@code stream()}
     * method implementation. Library utilities, e.g., {@code StreamSupplier.forEach[Ordered]()}, will play safe
     * and call {@code Stream.close()} after consuming the stream returned.
     *
     * @return fresh stream
     */
    Stream<T> stream();

    /**
     * Allows to use {@code StreamSupplier} as a {@code Supplier}. Result is the same as {@link #stream()}
     *
     * @return fresh stream
     */
    @Override
    default Stream<T> get() {
        return stream();
    }

    /**
     * Traverses the stream returned by {@link #stream()} in a non-deterministic order, executing the
     * action specified on each stream element. This method behaves as if {@link #stream()}{@code .forEach(action)}
     * is called, except that the stream returned by {@link #stream()} is {@link Stream#close() closed} after traversal.
     *
     * @param action action to execute for each stream element
     * @see #forEachOrdered(Consumer)
     * @see Stream#forEach(Consumer)
     */
    default void forEach(Consumer<? super T> action) {
        try (Stream<T> s = stream()) {
            s.forEach(action);
        }
    }

    /**
     * Traverses the stream returned by {@link #stream()} in stream <em>encounter order</em> (if stream has one),
     * executing the action specified on each stream element. This method behaves as if
     * {@link #stream()}{@code .forEachOrdered(action)} is called, except that the stream returned by {@link #stream()}
     * is {@link Stream#close() closed} after traversal.
     *
     * @param action action to execute for each stream element
     * @see #forEach(Consumer)
     * @see Stream#forEachOrdered(Consumer)
     */
    default void forEachOrdered(Consumer<? super T> action) {
        try (Stream<T> s = stream()) {
            s.forEachOrdered(action);
        }
    }
}
