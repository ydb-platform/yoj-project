package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.converter.StringValueType;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

public record MultiWrappedEntity2(
        @NonNull Id id
) implements RecordEntity<MultiWrappedEntity2> {
    public record Id(@NonNull MultiWrappedEntity2.WrapperOfIdStringValue value) implements Entity.Id<MultiWrappedEntity2> {
    }

    public record WrapperOfIdStringValue(@NonNull MultiWrappedEntity2.IdStringValue idWrapperValue) {
    }

    @Value
    @StringValueType
    public static class IdStringValue {
        @NonNull
        String regionCode;

        @NonNull
        String path;

        @NonNull
        public static MultiWrappedEntity2.IdStringValue fromString(@NonNull String serialized) {
            String[] parts = serialized.split(":");
            Preconditions.checkArgument(parts.length == 2, "Invalid ID: %s", serialized);
            return new IdStringValue(parts[0], parts[1]);
        }

        @NonNull
        @Override
        public String toString() {
            return regionCode + ":" + path;
        }
    }
}
