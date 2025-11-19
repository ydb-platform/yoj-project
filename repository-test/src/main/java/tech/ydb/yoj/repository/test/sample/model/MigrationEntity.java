package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.With;
import tech.ydb.yoj.repository.db.RecordEntity;

import javax.annotation.Nullable;
import java.time.Instant;

@With
public record MigrationEntity(
        @NonNull Id id,
        @Nullable String fillOnPostLoad,
        @Nullable Instant fillOnPreSave
) implements RecordEntity<MigrationEntity> {
    @NonNull
    @Override
    public MigrationEntity postLoad() {
        return this
                .postLoadTicket100500()
                //.postLoadTicket2001000()
                //etc...
                ;
    }

    @NonNull
    private MigrationEntity postLoadTicket100500() {
        return this
                .withFillOnPostLoad(fillOnPostLoad == null ? "Default Value" : fillOnPostLoad);
    }

    @NonNull
    @Override
    public MigrationEntity preSave() {
        return this
                .withFillOnPreSave(fillOnPreSave == null ? Instant.EPOCH : fillOnPreSave);
    }

    public record Id(String value) implements RecordEntity.Id<MigrationEntity> {
    }
}
