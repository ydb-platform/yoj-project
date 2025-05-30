package tech.ydb.yoj.util.function;

import lombok.RequiredArgsConstructor;

import java.util.function.Supplier;

@RequiredArgsConstructor(staticName = "of")
public final class LazyToString {
    private final Supplier<String> supplier;

    @Override
    public String toString() {
        return supplier.get();
    }
}
