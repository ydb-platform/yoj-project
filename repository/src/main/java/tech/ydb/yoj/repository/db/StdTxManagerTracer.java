package tech.ydb.yoj.repository.db;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import tech.ydb.yoj.repository.db.exception.RetryableException;
import tech.ydb.yoj.util.lang.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface StdTxManagerTracer {
    <T> T wrapTx(Supplier<T> supplier);

    <T> T wrapAttempt(String logContext, int attempt, Supplier<T> supplier);

    void onRetry(RetryableException e);

    void onException();

    void onRetryExceeded();

    void onLogSeparatePolicy();

    @RequiredArgsConstructor
    final class Default implements StdTxManagerTracer {
        private static final Logger log = LoggerFactory.getLogger(StdTxManagerTracer.class);

        private static final AtomicLong txLogIdSeq = new AtomicLong();

        private static final double[] TX_ATTEMPTS_BUCKETS = new double[]{
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 35, 40, 45, 50, 60, 70, 80, 90, 100
        };
        private static final double[] DURATION_BUCKETS = {
                .001, .0025, .005, .0075, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 25, 50, 75, 100
        };
        private static final Histogram totalDuration = Histogram.build()
                .name("tx_total_duration_seconds")
                .help("Tx total duration (seconds)")
                .labelNames("tx_name")
                .buckets(DURATION_BUCKETS)
                .register();
        private static final Histogram attemptDuration = Histogram.build()
                .name("tx_attempt_duration_seconds")
                .help("Tx attempt duration (seconds)")
                .labelNames("tx_name")
                .buckets(DURATION_BUCKETS)
                .register();
        private static final Histogram attempts = Histogram.build()
                .name("tx_attempts")
                .help("Tx attempts spent to success")
                .labelNames("tx_name")
                .buckets(TX_ATTEMPTS_BUCKETS)
                .register();
        private static final Counter results = Counter.build()
                .name("tx_result")
                .help("Tx commits/rollbacks/fails")
                .labelNames("tx_name", "result")
                .register();
        private static final Counter retries = Counter.build()
                .name("tx_retries")
                .help("Tx retry reasons")
                .labelNames("tx_name", "reason")
                .register();

        private final long txLogId = txLogIdSeq.incrementAndGet();

        private final TxOptions options;
        private final TxName txName;

        @Override
        public <T> T wrapTx(Supplier<T> supplier) {
            try (Histogram.Timer ignored = totalDuration.labels(txName.name()).startTimer()) {
                return supplier.get();
            }
        }

        @Override
        public <T> T wrapAttempt(String logContext, int attempt, Supplier<T> supplier) {
            attempts.labels(txName.name()).observe(attempt);

            T result;
            try (
                    var ignored1 = attemptDuration.labels(txName.name()).startTimer();
                    var ignored2 = MDC.putCloseable("tx", formatTx(txName, logContext));
                    var ignored3 = MDC.putCloseable("tx-id", formatTxId());
                    var ignored4 = MDC.putCloseable("tx-name", txName.logName()
            )) {
                result = supplier.get();
            }

            if (options.isDryRun()) {
                results.labels(txName.name(), "rollback").inc();
                results.labels(txName.name(), "dry_run").inc();
            } else {
                results.labels(txName.name(), "commit").inc();
            }

            return result;
        }

        @Override
        public void onRetry(RetryableException e) {
            retries.labels(txName.name(), getExceptionNameForMetric(e)).inc();
        }

        @Override
        public void onException() {
            results.labels(txName.name(), "rollback").inc();
        }

        @Override
        public void onRetryExceeded() {
            results.labels(txName.name(), "fail").inc();
        }

        @Override
        public void onLogSeparatePolicy() {
            log.warn("Transaction '{}' was run when another transaction is active. Perhaps unexpected behavior. "
                    + "Use TxManager.separate() to avoid this message", txName.logName()
            );
        }

        private static String getExceptionNameForMetric(RetryableException e) {
            return Strings.removeSuffix(e.getClass().getSimpleName(), "Exception");
        }

        private String formatTx(TxName txName, String logContext) {
            return formatTxId() + " {" + txName.logName() + (logContext != null ? "/" + logContext : "") + "}";
        }

        private String formatTxId() {
            String leftPad = Strings.leftPad(Long.toUnsignedString(txLogId, 36), 6, '0');
            return leftPad + options.getIsolationLevel().getTxIdSuffix();
        }
    }

    @RequiredArgsConstructor
    final class Composite implements StdTxManagerTracer {
        private final List<StdTxManagerTracer> tracers;

        public static Composite of(StdTxManagerTracer... tracers) {
            return new Composite(Arrays.asList(tracers));
        }

        @Override
        public <T> T wrapTx(Supplier<T> supplier) {
            return wrapAll(supplier, StdTxManagerTracer::wrapTx);
        }

        @Override
        public <T> T wrapAttempt(String logContext, int attempt, Supplier<T> supplier) {
            return wrapAll(supplier, (tracer, supp) -> tracer.wrapAttempt(logContext, attempt, supp));
        }

        @Override
        public void onRetry(RetryableException e) {
            runAll(t -> t.onRetry(e));
        }

        @Override
        public void onException() {
            runAll(StdTxManagerTracer::onException);
        }

        @Override
        public void onRetryExceeded() {
            runAll(StdTxManagerTracer::onRetryExceeded);
        }

        @Override
        public void onLogSeparatePolicy() {
            runAll(StdTxManagerTracer::onLogSeparatePolicy);
        }

        private void runAll(Consumer<StdTxManagerTracer> action) {
            for (StdTxManagerTracer tracer : tracers) {
                action.accept(tracer);
            }
        }

        private <T> T wrapAll(Supplier<T> supplier, BiFunction<StdTxManagerTracer, Supplier<T>, T> action) {
            for (int i = tracers.size() - 1; i >= 0; i--) {
                Supplier<T> finalSupplier = supplier;
                StdTxManagerTracer tracer = tracers.get(i);
                supplier = () -> action.apply(tracer, finalSupplier);
            }
            return supplier.get();
        }
    }
}
