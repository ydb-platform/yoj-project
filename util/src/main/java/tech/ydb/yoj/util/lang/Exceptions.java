package tech.ydb.yoj.util.lang;

import lombok.NonNull;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Collections.newSetFromMap;

public final class Exceptions {
    private Exceptions() {
    }

    public static <X extends Throwable> boolean isOrCausedBy(@Nullable Throwable t, @NonNull Class<X> causeClass) {
        // Inspired by Throwable#printStackTrace()
        Set<Throwable> dejaVu = newSetFromMap(new IdentityHashMap<>());

        Throwable current = t;
        while (current != null) {
            if (causeClass.isInstance(current)) {
                return true;
            }

            dejaVu.add(current);

            Throwable next = current.getCause();
            if (dejaVu.contains(next)) {
                // Found loop in the causal chain
                return false;
            }
            current = next;
        }
        return false;
    }

    public static <X extends Throwable> boolean isOrCausedByExact(@Nullable Throwable t, @NonNull Class<X> causeClass) {
        // Inspired by Throwable#printStackTrace()
        Set<Throwable> dejaVu = newSetFromMap(new IdentityHashMap<>());

        Throwable current = t;
        while (current != null) {
            if (causeClass.equals(current.getClass())) {
                return true;
            }

            dejaVu.add(current);

            Throwable next = current.getCause();
            if (dejaVu.contains(next)) {
                // Found loop in the causal chain
                return false;
            }
            current = next;
        }
        return false;
    }

    /**
     * Tries to close all the specified {@link AutoCloseable} resources, collecting all exceptions received and re-throwing
     * them as (first exception with all other exceptions in suppresssed list).
     *
     * @param closeables resources to close
     * @see #closeAll(List)
     */
    public static void closeAll(@NonNull AutoCloseable... closeables) {
        closeAll(Arrays.asList(closeables));
    }

    /**
     * Tries to close all the specified {@link AutoCloseable} resources, collecting all exceptions received and re-throwing
     * them as (first exception with all other exceptions in suppresssed list).
     *
     * @param closeables list of resources to close
     * @see #closeAll(AutoCloseable...)
     */
    @SneakyThrows
    public static void closeAll(@NonNull List</*@Nullable*/ AutoCloseable> closeables) {
        Exception exception = null;
        for (AutoCloseable c : closeables) {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }
}
