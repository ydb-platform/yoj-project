package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.converter.StringValueType;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

public record Membership(@NonNull Id id) implements RecordEntity<Membership> {
    public record Id(@NonNull StringValue nid) implements Entity.Id<Membership> {
    }

    @StringValueType
    public record StringValue(@NonNull String value) {
        public StringValue {
            Preconditions.checkArgument(value.endsWith("-peach"), "Peach is very unhappy!");
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
