package tech.ydb.yoj.util.retry;

import lombok.NonNull;

import java.time.Duration;

public interface RetryPolicy {
    /**
     * @param attempt failed attempt number, counting from 1
     * @return recommended retry interval
     */
    Duration calcDuration(int attempt);

    boolean isSameAs(@NonNull RetryPolicy other);

    static RetryPolicy expBackoff() {
        return ExponentialBackoffRetryPolicy.DEFAULT;
    }

    @NonNull
    static RetryPolicy expBackoff(long initial, long max, double jitter, double multiplier) {
        return new ExponentialBackoffRetryPolicy(initial, max, jitter, multiplier);
    }

    @NonNull
    static RetryPolicy expBackoff(Duration initial, Duration max, double jitter, double multiplier) {
        return new ExponentialBackoffRetryPolicy(initial.toMillis(), max.toMillis(), jitter, multiplier);
    }

    @NonNull
    static RetryPolicy fixed(Duration delay) {
        return fixed(delay.toMillis());
    }

    @NonNull
    static RetryPolicy fixed(Duration delay, double jitter) {
        return fixed(delay.toMillis(), jitter);
    }

    @NonNull
    static RetryPolicy fixed(long delay) {
        return fixed(delay, 0);
    }

    @NonNull
    static RetryPolicy fixed(long delay, double jitter) {
        return new FixedDelayRetryPolicy(delay, jitter);
    }

    static RetryPolicy retryImmediately() {
        return new FixedDelayRetryPolicy(0L, 0.0);
    }
}
