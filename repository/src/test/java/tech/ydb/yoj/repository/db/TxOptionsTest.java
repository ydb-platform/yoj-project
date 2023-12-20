package tech.ydb.yoj.repository.db;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TxOptionsTest {
    @Test
    public void testMinTimeoutOptionsReturnsDefaultIfNoTimeoutIsProvided() {
        var options = TxOptions
                .create(IsolationLevel.ONLINE_CONSISTENT_READ_ONLY);

        assertThat(options.minTimeoutOptions(null)).isEqualTo(TxOptions.TimeoutOptions.DEFAULT);
    }

    @Test
    public void testMinTimeoutOptionsReturnsMinTimeoutOptions() {
        var options1 = TxOptions
                .create(IsolationLevel.ONLINE_CONSISTENT_READ_ONLY)
                .withTimeoutOptions(new TxOptions.TimeoutOptions(Duration.ofNanos(1)));

        assertThat(options1.minTimeoutOptions(Duration.ofNanos(2)).getTimeout()).isEqualTo(Duration.ofNanos(1));

        var options2 = TxOptions
                .create(IsolationLevel.ONLINE_CONSISTENT_READ_ONLY)
                .withTimeoutOptions(new TxOptions.TimeoutOptions(Duration.ofNanos(2)));

        assertThat(options2.minTimeoutOptions(Duration.ofNanos(1)).getTimeout()).isEqualTo(Duration.ofNanos(1));
    }

    @Test(expected = NullPointerException.class)
    public void testTimeoutOptionsCantAcceptNullTimeout() {
        new TxOptions.TimeoutOptions(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTimeoutOptionsCantBeChangedToNull() {
        var to = new TxOptions.TimeoutOptions(Duration.ofMillis(3));
        to.withTimeout(null);
    }

    @Test
    public void testCancelAfterAndDeadlineIsNotNullIfTimeoutIsSet() {
        var timeoutOptions = new TxOptions.TimeoutOptions(Duration.ofMillis(100));

        assertThat(timeoutOptions.getTimeout()).isNotNull();
        assertThat(timeoutOptions.getCancelAfter()).isNotNull();
        assertThat(timeoutOptions.getDeadlineAfter()).isNotNull();
    }

    @Test
    public void testCancelAfterDiffIsNotLessThan50Millis() {
        long MIN_DIFF_MILLIS = 50L;
        var timeoutOptions = new TxOptions.TimeoutOptions(Duration.ofMillis(100));

        Duration diff = timeoutOptions.getTimeout().minus(timeoutOptions.getCancelAfter());
        assertThat(diff.toMillis()).isEqualTo(MIN_DIFF_MILLIS);
    }

    @Test
    public void testCancelAfterDiffIsNotGreaterThan100Millis() {
        long maxDiffMillis = 100L;
        long fiveMinutes = 5;

        var timeoutOptions = new TxOptions.TimeoutOptions(Duration.ofMinutes(fiveMinutes));

        Duration diff = timeoutOptions.getTimeout().minus(timeoutOptions.getCancelAfter());
        assertThat(diff.toMillis()).isEqualTo(maxDiffMillis);
    }

    @Test
    public void testCancelAfterCantBeNegative() {
        var timeoutOptions = new TxOptions.TimeoutOptions(Duration.ofNanos(1L));
        assertThat(timeoutOptions.getCancelAfter()).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void testDeadlineAndTimeoutAreTheSame() {
        Duration timeout = Duration.ofMillis(100L);
        long maxDiffNanos = 5_000_000;
        var timeoutOptions = new TxOptions.TimeoutOptions(timeout);

        long deadLineRemainingNanos = timeoutOptions.getDeadlineAfter() - System.nanoTime();
        long timeoutRemainingNanos = timeoutOptions.getTimeout().toNanos();
        assertThat(deadLineRemainingNanos).isBetween(timeoutRemainingNanos - maxDiffNanos, timeoutRemainingNanos + maxDiffNanos);
    }
}
