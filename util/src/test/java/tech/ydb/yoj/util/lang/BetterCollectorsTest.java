package tech.ydb.yoj.util.lang;

import org.junit.Test;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class BetterCollectorsTest {
    @Test
    public void basicNullMapping() {
        Map<Integer, Integer> result = Stream.of(1, 2, 3)
                .collect(BetterCollectors.toMapNullFriendly(Function.identity(), x -> x % 2 == 1 ? x : null));

        assertThat(result)
                .hasSize(3)
                .containsEntry(1, 1)
                .containsEntry(2, null)
                .containsEntry(3, 3);
    }

    @Test
    public void mappingNonnullToNullRemains() {
        var collected = singletonMap("k1", null).entrySet().stream().collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue));
        assertThat(collected).isEqualTo(singletonMap("k1", null));
    }

    @Test
    public void mappingNullToNullRemains() {
        var collected = singletonMap(null, null).entrySet().stream().collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue));
        assertThat(collected).isEqualTo(singletonMap(null, null));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void doNotOverwriteMappingToNull() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream.of(new SimpleImmutableEntry<>("k1", null), new SimpleImmutableEntry<>("k1", 150))
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue))
                )
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: k1"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void duplicateKeys() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream.of(1, 2, 3, 1)
                        .collect(BetterCollectors.toMapNullFriendly(Function.identity(), x -> x % 2 == 1 ? x : null)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: 1"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void duplicateNullKeys() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream.of(1, 2, 3).collect(BetterCollectors.toMapNullFriendly(i -> null, i -> i)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void duplicateNullKeysAndValuesNullFirst() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream
                        .of(new SimpleImmutableEntry<>(null, null), new SimpleImmutableEntry<>(null, 5))
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void duplicateNullKeysAndValuesNullSecond() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream
                        .of(new SimpleImmutableEntry<>(null, 5), new SimpleImmutableEntry<>(null, null))
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }

    @Test
    public void parallel() {
        Map<Integer, Integer> result = Stream.of(1, 2, 3)
                .parallel() // this causes .combiner() to be called
                .collect(BetterCollectors.toMapNullFriendly(Function.identity(), x -> x % 2 == 1 ? x : null));

        assertThat(result)
                .hasSize(3)
                .containsEntry(1, 1)
                .containsEntry(2, null)
                .containsEntry(3, 3);
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void parallelWithDuplicateKeys() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream.of(1, 2, 3, 1, 2, 3)
                        .parallel() // this causes .combiner() to be called
                        .collect(BetterCollectors.toMapNullFriendly(Function.identity(), x -> x % 2 == 1 ? x : null)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: 1"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void parallelDoNotOverwriteMappingToNull() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream
                        .of(new SimpleImmutableEntry<>("k1", null), new SimpleImmutableEntry<>("k1", 150))
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue))
                )
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: k1"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void parallelDuplicateNullKeys() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream.of(1, 2, 3).parallel().collect(BetterCollectors.toMapNullFriendly(i -> null, i -> i)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void parallelDuplicateNullKeysAndValuesNullFirst() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream
                        .of(new SimpleImmutableEntry<>(null, null), new SimpleImmutableEntry<>(null, 5))
                        .parallel()
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void parallelDuplicateNullKeysAndValuesNullSecond() {
        assertThatIllegalStateException()
                .isThrownBy(() -> Stream
                        .of(new SimpleImmutableEntry<>(null, 5), new SimpleImmutableEntry<>(null, null))
                        .parallel()
                        .collect(BetterCollectors.toMapNullFriendly(Entry::getKey, Entry::getValue)))
                .satisfies(ex -> assertThat(ex).hasMessageContaining("Duplicate key: null"));
    }
}
