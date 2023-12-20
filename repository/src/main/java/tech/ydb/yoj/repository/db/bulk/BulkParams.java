package tech.ydb.yoj.repository.db.bulk;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class BulkParams {
    public static final BulkParams DEFAULT = BulkParams.builder().build();

    @Builder.Default
    Duration timeout = Duration.ofSeconds(60);

    @Builder.Default
    Duration cancelAfter = null;

    @Builder.Default
    long deadlineAfter = 0;

    @Builder.Default
    String traceId = null;
}
