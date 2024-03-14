package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;

public record VersionedEntity(
        Id id,
        @Column(
                customValueType = @CustomValueType(
                        columnClass = Long.class,
                        converter = Version.Converter.class
                )
        )
        Version version2
) implements RecordEntity<VersionedEntity> {
    public record Id(
            String value,
            @Column(
                    customValueType = @CustomValueType(
                            columnClass = Long.class,
                            converter = Version.Converter.class
                    )
            )
            Version version
    ) implements Entity.Id<VersionedEntity> {
    }
}
