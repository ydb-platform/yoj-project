package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import tech.ydb.yoj.databind.converter.StringColumn;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

import javax.annotation.Nullable;

public record MultiWrappedEntity(
        @NonNull Id id,
        @NonNull String payload,
        @Nullable OptionalPayload optionalPayload
) implements RecordEntity<MultiWrappedEntity> {
    public record Id(
            @StringColumn StringWrapper itIsReallyString
    ) implements Entity.Id<MultiWrappedEntity> {
    }

    public record StringWrapper(@NonNull String value) {
        @Override
        public String toString() {
            return value;
        }
    }

    public record OptionalPayload(
            @StringColumn @NonNull StringWrapper wrapper
    ) {
    }
}
