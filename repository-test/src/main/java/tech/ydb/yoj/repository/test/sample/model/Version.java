package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

public record Version(long value) {
    public static final class Converter implements ValueConverter<Version, Long> {
        @Override
        public @NonNull Long toColumn(@NonNull JavaField field, @NonNull Version v) {
            return v.value();
        }

        @Override
        public @NonNull Version toJava(@NonNull JavaField field, @NonNull Long value) {
            return new Version(value);
        }
    }
}
