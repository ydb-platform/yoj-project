package tech.ydb.yoj.util.lang;

import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Base interface for objects that have a loggable <em>debug representation</em> that's possibly different
 * from {@link Object#toString() toString()} and/or lazy.
 */
public interface DebugLoggable {
    /**
     * Returns a representation of {@code this} for debug logging purposes, potentially a lazy one.
     * <p>Calling {@code toString()} on the <em>result</em> of this method should produce a string representation
     * of {@code this}.
     * <p>Default implementation just returns {@code this}.
     *
     * @return string representation of {@code this} for debugging; must not be {@code null}
     */
    @NonNull
    default Object toLoggable() {
        return this;
    }

    /**
     * Returns representation of the specified object {@code obj} for debug logging purposes, as follows:
     * <ul>
     * <li>If {@code obj} is a {@link DebugLoggable}, return the result of {@link #toLoggable() obj.toLoggable()}.
     * If method returned {@code null} by mistake, return the literal string {@code "null"}.
     * <li>If {@code obj} is an {@link Iterable}, return a string depending on its size:
     * <ul>
     * <li>for an empty {@link Iterable}, return the literal string {@code "[]"},</li>
     * <li>for a single-element {@link Iterable}, return the result of calling {@code toLoggable()} on that element,
     * </li>
     * <li>for larger {@link Iterable}s that don't implement {@link Collection}, return a string of the form
     * {@code "[L(1), ...]"} where {@code L(1)} is the result of recursively applying {@code toLoggable()}
     * to the first element of the {@link Iterable},
     * </li>
     * <li>for larger {@link Iterable}s that implement {@code Collection}, return a string of the form
     * {@code "[L(1), ...](S)"} where {@code L(1)} is the result of recursively applying {@code toLoggable()}
     * to the first element of the {@link Iterable}, and {@code S} is the result of calling {@link Collection#size()}.
     * </li>
     * <li>If {@code obj} is {@code null} but not a {@link DebugLoggable} or an {@link Iterable} either,
     * just return {@code obj} itself.</li>
     * <li>If {@code obj} is {@code null}, return the literal string {@code "null"}.</li>
     * </ul>
     *
     * <p>If {@code obj} an {@link Iterable}, the first element (if any) will be printed, and if it's
     * also a {@link Collection}, then the size will be printed, as well.
     *
     * @param obj object to return loggable debug representation for; may be {@code null}
     * @return loggable debug representation for the object; never {@code null}
     */
    @NonNull
    static Object toLoggable(@Nullable Object obj) {
        if (obj instanceof DebugLoggable dl) {
            return lenientToLoggable(dl);
        } else if (obj instanceof Iterable<?> iterable) {
            return toLoggable(iterable);
        } else if (obj != null) {
            return obj;
        } else {
            return "null";
        }
    }

    /**
     * Returns a verbose representation of the specified object {@code obj} for debug logging purposes, as follows:
     * <ul>
     * <li>If {@code obj} is a {@link DebugLoggable}, return the result of {@link #toLoggable() obj.toLoggable()}.
     * If method returned {@code null} by mistake, return the literal string {@code "null"}.
     * </li>
     * <li>If {@code obj} is an {@link Iterable}, return an {@code AbstractCollection.toString()}-inspired string
     * representation {@code "[L(1), L(2), ..., L(N)]"} where {@code L(i)} is the result of recursively applying
     * {@code toVerboseLoggable()} to the {@code i}-th element of the {@code Iterable}.
     * </li>
     * <li>If {@code obj} is {@code null} but not a {@link DebugLoggable} or an {@link Iterable} either,
     * just return {@code obj} itself.</li>
     * <li>If {@code obj} is {@code null}, return the literal string {@code "null"}.</li>
     * </ul>
     *
     * @param obj object to return verbose loggable debug representation for; may be {@code null}
     * @return verbose loggable debug representation for the object; never {@code null}
     */
    @NonNull
    static Object toVerboseLoggable(@Nullable Object obj) {
        if (obj instanceof DebugLoggable dl) {
            return lenientToLoggable(dl);
        } else if (obj instanceof Iterable<?> iterable) {
            return toVerboseLoggable(iterable);
        } else if (obj != null) {
            return obj;
        } else {
            return "null";
        }
    }

    @NonNull
    private static Object toLoggable(@NonNull Iterable<?> iterable) {
        var iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            return "[]";
        } else {
            var sb = new StringBuilder();
            var first = iterator.next();
            boolean hasMore = iterator.hasNext();

            if (hasMore) {
                sb.append('[');
            }
            sb.append(toLoggable(first));
            if (hasMore) {
                sb.append(", ...]");
                if (iterable instanceof Collection<?> collection) {
                    sb.append('(').append(collection.size()).append(')');
                }
            }

            return sb.toString();
        }
    }

    @NonNull
    private static Object toVerboseLoggable(@NonNull Iterable<?> iterable) {
        var iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            return "[]";
        } else {
            var sb = new StringBuilder();
            var first = iterator.next();
            boolean hasMoreThanOne = iterator.hasNext();

            if (hasMoreThanOne) {
                sb.append('[');
            }
            sb.append(toVerboseLoggable(first));
            if (hasMoreThanOne) {
                while (iterator.hasNext()) {
                    sb.append(", ");
                    sb.append(toVerboseLoggable(iterator.next()));
                }
                sb.append(']');
            }

            return sb.toString();
        }
    }

    @NonNull
    private static Object lenientToLoggable(@NonNull DebugLoggable dl) {
        try {
            var l = dl.toLoggable();
            // Do NOT simplify (l == null) to `false`: We're trying to catch people who ignored `@NonNull` annotation
            // on DebugLoggable.toLoggable().
            return l == null ? "null" : l;
        } catch (Exception e) {
            String objectToString = dl.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(dl));
            return "(" + objectToString + ".toLoggable() threw " + e.getClass().getName() + ")";
        }
    }
}
