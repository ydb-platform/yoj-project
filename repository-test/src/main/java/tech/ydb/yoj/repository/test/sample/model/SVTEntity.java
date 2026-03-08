package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.converter.StringValueType;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;

@Value
public class SVTEntity implements Entity<SVTEntity> {
    Id id;

    @Nullable
    SVT c;

    @Value
    public static class Id implements Entity.Id<SVTEntity> {
        @NonNull
        String a;

        @NonNull
        SVT b;
    }

    @StringValueType
    public record SVT(String value) {
        @Override
        public String toString() {
            return value;
        }
    }
}