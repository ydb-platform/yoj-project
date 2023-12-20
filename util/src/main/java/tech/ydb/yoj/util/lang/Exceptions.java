package tech.ydb.yoj.util.lang;

import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.IdentityHashMap;
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
}
