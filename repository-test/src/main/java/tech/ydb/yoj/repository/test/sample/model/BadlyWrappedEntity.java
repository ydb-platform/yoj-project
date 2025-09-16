package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.converter.StringValueType;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import javax.annotation.Nullable;

public record BadlyWrappedEntity(
        @NonNull Id id,
        @Nullable BadStringValueWrapper badWrapper
) implements RecordEntity<BadlyWrappedEntity> {
    public record Id(String value) implements Entity.Id<BadlyWrappedEntity> {
    }

    @StringValueType
    @RequiredArgsConstructor
    public static final class BadStringValueWrapper {
        private final String value;

        @NonNull
        public static BadStringValueWrapper valueOf(@NonNull String str) {
            return new BadStringValueWrapper(str);
        }

        @Override
        public String toString() {
            // MWAHAHAHAHAHAH!!! StringValueConverter doesn't expect that, for sure!
            return value;
        }
    }
}
