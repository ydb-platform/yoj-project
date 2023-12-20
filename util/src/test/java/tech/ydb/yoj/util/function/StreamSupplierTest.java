package tech.ydb.yoj.util.function;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class StreamSupplierTest {
    @Test
    public void supplierIsReusable() {
        List<String> lst = List.of("hello", "world");
        StreamSupplier<String> streamSupplier = lst::stream;

        List<String> seenThruStream = new ArrayList<>();
        List<String> seenThruStreamSupplier = new ArrayList<>();
        Stream<String> stream = lst.stream();
        stream.forEachOrdered(seenThruStream::add);
        streamSupplier.forEachOrdered(seenThruStreamSupplier::add);
        assertThat(seenThruStream).isEqualTo(lst);
        assertThat(seenThruStreamSupplier).isEqualTo(lst);

        assertThatIllegalStateException().isThrownBy(() -> stream.forEachOrdered(seenThruStream::add));
        streamSupplier.forEachOrdered(seenThruStreamSupplier::add);
        assertThat(seenThruStreamSupplier).isEqualTo(List.of("hello", "world", "hello", "world"));
    }

    @Test
    public void closeIsCalledByForEach() {
        AtomicBoolean closeCalled = new AtomicBoolean();
        StreamSupplier<?> streamSupplier = () -> Stream.empty().onClose(() -> closeCalled.set(true));
        streamSupplier.forEach(ignore -> {
            // NOOP;
        });
        assertThat(closeCalled).isTrue();
    }

    @Test
    public void closeIsCalledByForEachOrdered() {
        AtomicBoolean closeCalled = new AtomicBoolean();
        StreamSupplier<?> streamSupplier = () -> Stream.empty().onClose(() -> closeCalled.set(true));
        streamSupplier.forEachOrdered(ignore -> {
            // NOOP;
        });
        assertThat(closeCalled).isTrue();
    }
}
