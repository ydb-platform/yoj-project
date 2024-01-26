package tech.ydb.yoj.repository.db.readtable;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class ReadTableParams<ID> {
    boolean ordered;
    ID fromKey;
    boolean fromInclusive;
    ID toKey;
    boolean toInclusive;
    int rowLimit;
    @Builder.Default
    Duration timeout = Duration.ofSeconds(60);

    boolean useNewSpliterator;

    int batchLimitBytes;
    int batchLimitRows;

    public static <ID> ReadTableParams<ID> getDefault() {
        return ReadTableParams.<ID>builder().build();
    }

    public static class ReadTableParamsBuilder<ID> {
        public ReadTableParams.ReadTableParamsBuilder<ID> ordered() {
            this.ordered = true;
            return this;
        }

        public ReadTableParams.ReadTableParamsBuilder<ID> fromKeyInclusive(ID fromKey) {
            return fromKey(fromKey).fromInclusive(true);
        }

        public ReadTableParams.ReadTableParamsBuilder<ID> toKeyInclusive(ID toKey) {
            return toKey(toKey).toInclusive(true);
        }
    }
}
