package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.base.CaseFormat;
import lombok.NonNull;
import lombok.With;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.DbTypeQualifier;
import tech.ydb.yoj.repository.db.RecordEntity;

import javax.annotation.Nullable;

public record EnumEntity(
        @NonNull
        Id id,

        @With
        @Nullable
        IpVersion ipVersion,

        @With
        @Nullable
        @Column(dbTypeQualifier = DbTypeQualifier.ENUM_TO_STRING)
        NetworkType networkType
) implements RecordEntity<EnumEntity> {
    public record Id(@NonNull String value) implements RecordEntity.Id<EnumEntity> {
    }

    public enum IpVersion {
        IPV4,
        IPV6,
    }

    public enum NetworkType {
        OVERLAY,
        UNDERLAY_V4,
        UNDERLAY_V6,
        ;

        @NonNull
        @Override
        public String toString() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, name());
        }
    }
}
