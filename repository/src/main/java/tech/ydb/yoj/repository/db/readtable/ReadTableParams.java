package tech.ydb.yoj.repository.db.readtable;

import lombok.Builder;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;

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

    /**
     * Set this to {@code true} to use a {@code Spliterator} contract-conformant and less memory consuming implementation for the {@code Stream}
     * returned by {@code readTable()}.
     * <p>Note that using the new implementation currently has a negative performance impact, for more information refer to
     * <a href="https://github.com/ydb-platform/yoj-project/issues/42">GitHub Issue #42</a>.
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/42")
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
