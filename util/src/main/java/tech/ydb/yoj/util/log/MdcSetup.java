package tech.ydb.yoj.util.log;

import lombok.NonNull;
import org.slf4j.MDC;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;

/// This class supports automatic setting and cleanup of [MDC (Mapped Diagnostic Context)][MDC] key-value pairs.
/// It ensures that MDC entries are present when an exception occurs and are cleaned up after the exception is logged.
///
/// This is much more useful than the [org.slf4j.MDC.MDCCloseable]'s `try-with-resources` behavior,
/// because **MDC entries established with `MDCCloseable`s in the `try (...)` are `remove()`d before
/// the `catch {}` and `finally {}` blocks of `try-with-resources` are reached**.
///
/// ----
///
/// This class is most similar in idea and function to the proposed slf4j 2.1.x [MDCAmbit](
/// https://www.slf4j.org/apidocs-2.1.0-alpha0/org.slf4j/org/slf4j/MDCAmbit.html) class, but
/// it restores {@link #put(String, Object) established} entries to their state **before the creation of `MdcSetup`**,
/// and not just blindly `remove()`s them on [#restore()].
///
/// Typical usage of `MdcSetup` to run with the specified MDC key-value pairs:
/// ```java
/// MdcSetup mdcs = new MdcSetup();
/// try {
///     mdcs.put("k0", "v0")
///         .put("k1", "v1");
///     doSomething();
///     mdcs.put("k2, "v2");
///     doSomethingThatThrowsException();
/// } catch (Exception e) {
///     // Here MDC.get("k0") would return "v0",
///     // MDC.get("k1") will return "v1",
///     // and MDC.get("k2") would return "v2"
/// } finally {
///     // Here MDC values for keys "k0", "k1" and "k2" would return to their state before **the creation of MdcSetup**
///     mdcs.restore();
/// }
/// ```
///
/// @see #put(String, Object)
/// @see #restore()
/// @see <a href="https://github.com/qos-ch/slf4j/discussions/372">slf4j #372</a>
@NotThreadSafe
public final class MdcSetup {
    private final Map<String, String> prevState = new HashMap<>();

    /// Sets a new value for an MDC entry with the specified `key`.
    ///
    /// On the first call to `put()` with the specified `key`, `MdcSetup` will remember the value (if any) for
    /// the corresponding MDC entry. That value will be restored when the [#restore()] method is called.
    ///
    /// @param key   MDC entry's key
    /// @param value new value to set for `key`
    /// @return this `MdcSetup` instance, to chain `put()` calls
    ///
    /// @see #restore()
    @NonNull
    public MdcSetup put(@NonNull String key, @NonNull Object value) {
        if (!prevState.containsKey(key)) {
            var prevValue = MDC.get(key);
            prevState.put(key, prevValue);
        }

        MDC.put(key, value.toString());
        return this;
    }

    /// Restores the values of MDC entries [established][#put(String, Object)] by this `MdcStack` to their original
    /// state (which was observed on first call to [put()][#put(String, Object)] for each key):
    /// - If there was a previous value for an MDC entry, it will be [restored][org.slf4j.MDC#put(String, String)]
    /// - If there was no previous value for an MDC entry, the entry will be [removed][org.slf4j.MDC#remove(String)]
    public void restore() {
        for (var entry : prevState.entrySet()) {
            String key = entry.getKey();
            String prevValue = entry.getValue();
            if (prevValue == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, prevValue);
            }
        }
    }
}
