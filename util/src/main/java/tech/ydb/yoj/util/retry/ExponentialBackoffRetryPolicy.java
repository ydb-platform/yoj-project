package tech.ydb.yoj.util.retry;

import com.google.common.base.Preconditions;
import lombok.Value;

import javax.annotation.Nullable;
import java.beans.ConstructorProperties;
import java.time.Duration;

@Value
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    private static final long NANOS_IN_MILLIS = 1_000_000L;
    public static final RetryPolicy DEFAULT = new ExponentialBackoffRetryPolicy(1000L, 120_000L, 0.2, 1.6);

    long initial;
    long max;
    double jitter;
    double multiplier;

    @ConstructorProperties({"initial", "max", "jitter", "multiplier"})
    public ExponentialBackoffRetryPolicy(long initial, long max, double jitter, double multiplier) {
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be at least 1");
        Preconditions.checkArgument(initial >= 0, "initial delay must be non-negative");
        Preconditions.checkArgument(max >= 0, "max delay must be non-negative");
        Preconditions.checkArgument(jitter >= 0 && jitter <= 1, "jitter must be a ratio between 0 and 1, inclusive");

        this.initial = initial;
        this.max = max;
        this.jitter = jitter;
        this.multiplier = multiplier;
    }

    @Override
    public Duration calcDuration(int attempt) {
        Preconditions.checkArgument(attempt >= 0, "attempt must be greater than 0");
        if (attempt == 0) {
            return Duration.ofMillis(Math.min(max, initial));
        }
        double delay = Math.min(max, initial * Math.pow(multiplier, attempt - 1));
        return Duration.ofNanos((long) (delay * NANOS_IN_MILLIS * (1 + jitter * (Math.random() * 2 - 1))));
    }

    @Override
    public boolean isSameAs(@Nullable RetryPolicy other) {
        return this.equals(other);
    }
}
