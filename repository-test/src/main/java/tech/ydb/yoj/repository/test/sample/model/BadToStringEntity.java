package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.util.concurrent.Uninterruptibles;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;

@Value
@EqualsAndHashCode(doNotUseGetters = true)
public class BadToStringEntity implements Entity<BadToStringEntity> {
    @NonNull
    Id id;

    @Nullable
    Duration toStringDuration;

    @SuppressWarnings("unused")
    public Duration getToStringDuration() {
        // Mwahahahahahah #1: NPE if a nullable field is null
        Objects.requireNonNull(toStringDuration, "toStringDuration");
        // Mwahahahahahah #2: Delay if a nullable field is not null
        Uninterruptibles.sleepUninterruptibly(toStringDuration);

        return toStringDuration;
    }

    public record Id(@NonNull String id) implements Entity.Id<BadToStringEntity> {
    }
}
