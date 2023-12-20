package tech.ydb.yoj.util.lang;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;

/**
 * Better versions of collectors from {@link java.util.stream.Collectors java.util.stream.Collectors}.
 */
public final class BetterCollectors {
    private BetterCollectors() {
    }

    /**
     * A variant of {@link Collectors#toMap(Function, Function) Collectors.toMap()} that allows
     * {@code valueMapper} to produce {@code null} values for map entries.
     *
     * @param keyMapper   a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param <T>         the type of input elements
     * @param <K>         the type of keys
     * @param <U>         the type of values
     * @return a map produced from the source stream, possibly with null values
     * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8148463">JDK-8148463 Bug</a>
     * @see <a href="https://stackoverflow.com/a/43299462">The Workaround</a>
     */
    public static <T, K, U> Collector<T, ?, Map<K, U>> toMapNullFriendly(Function<? super T, ? extends K> keyMapper,
                                                                         Function<? super T, ? extends U> valueMapper) {
        return Collector.of(
                LinkedHashMap::new,
                (map, elem) -> {
                    K key = keyMapper.apply(elem);
                    Preconditions.checkState(!map.containsKey(key), "Duplicate key: %s", key);
                    map.put(key, valueMapper.apply(elem));
                },
                (m1, m2) -> {
                    for (var e : m2.entrySet()) {
                        Preconditions.checkState(!m1.containsKey(e.getKey()), "Duplicate key: %s", e.getKey());
                        m1.put(e.getKey(), e.getValue());
                    }
                    return m1;
                },
                IDENTITY_FINISH
        );
    }
}
