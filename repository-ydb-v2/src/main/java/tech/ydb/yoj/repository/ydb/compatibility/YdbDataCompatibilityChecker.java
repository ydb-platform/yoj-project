package tech.ydb.yoj.repository.ydb.compatibility;

import com.google.common.base.Stopwatch;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.exception.SchemaException;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.ydb.YdbRepository;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;

public final class YdbDataCompatibilityChecker {
    private static final Logger log = LoggerFactory.getLogger(YdbSchemaCompatibilityChecker.class);

    private final YdbRepository repository;
    private final List<Class<? extends Entity>> entities;
    private final Config config;

    public YdbDataCompatibilityChecker(List<Class<? extends Entity>> entities, YdbRepository repository, Config config) {
        this.entities = entities;
        this.repository = repository;
        this.config = config;
    }

    public void run() {
        ReadTableParams.ReadTableParamsBuilder<Object> paramsBuilder = ReadTableParams.builder();
        if (config.rowLimit > 0) {
            paramsBuilder.rowLimit(config.rowLimit);
            paramsBuilder.ordered();
        }
        ReadTableParams<Object> params = paramsBuilder.timeout(config.timeout).build();
        TxManager txManager = new StdTxManager(repository);
        Stopwatch totalTime = Stopwatch.createStarted();
        Stream<Class<? extends Entity>> stream = entities.stream();
        if (config.parallel) {
            stream = stream.parallel();
        }
        stream.forEach(ec -> {
            log.info(format("Checking entities of %s", ec.getSimpleName()));
            Stopwatch sw = Stopwatch.createStarted();
            try {
                txManager.readOnly().noFirstLevelCache().run(() -> {
                    RepositoryTransaction tx = Tx.Current.get().getRepositoryTransaction();

                    @SuppressWarnings("unchecked")
                    long checkedCount = tx.table(ec).readTable(params).count();

                    log.info(format("[%s] Checked %d entities of %s", sw, checkedCount, ec.getSimpleName()));
                });
            } catch (Exception e) {
                String message = format("[%s] Got exception while checking entities of %s: ", sw, ec.getSimpleName());
                if (config.skipSchemaErrors && e instanceof SchemaException) {
                    log.warn(message);
                } else {
                    log.error(message);
                    throw e;
                }
            }
        });
        log.info(format("[%s] Data compatibility checked successfully", totalTime));
    }

    @Value
    @Builder
    @With
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Config {
        public static final Config DEFAULT = Config.builder().build();

        @Builder.Default
        int rowLimit = 5000;
        @Builder.Default
        Duration timeout = Duration.ofMinutes(1);
        @Builder.Default
        boolean parallel = true;
        @Builder.Default
        boolean skipSchemaErrors = false;
    }
}
