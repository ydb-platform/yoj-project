package tech.ydb.yoj.util.retry;

import lombok.RequiredArgsConstructor;
import lombok.Value;

import javax.annotation.Nullable;
import java.time.Duration;

import static lombok.AccessLevel.PACKAGE;

@Value
@RequiredArgsConstructor(access = PACKAGE)
public class FixedDelayRetryPolicy implements RetryPolicy {
    long delay;
    double jitter;

    @Override
    public Duration calcDuration(int attempt) {
        return Duration.ofMillis((long) (delay * (1 + jitter * (Math.random() * 2 - 1))));
    }

    @Override
    public boolean isSameAs(@Nullable RetryPolicy other) {
        return this.equals(other);
    }
}
