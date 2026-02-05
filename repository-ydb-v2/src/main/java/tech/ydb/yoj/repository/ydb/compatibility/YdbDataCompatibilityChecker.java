package tech.ydb.yoj.repository.ydb.compatibility;

import com.google.common.base.Stopwatch;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.exception.SchemaException;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.ydb.YdbRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;

@RequiredArgsConstructor
public final class YdbDataCompatibilityChecker {
    private static final Logger log = LoggerFactory.getLogger(YdbSchemaCompatibilityChecker.class);

    private final YdbRepository repository;
    private final List<TableDescriptor<?>> tableDescriptors;
    private final Config config;

    public YdbDataCompatibilityChecker(List<Class<? extends Entity>> entities, YdbRepository repository, Config config) {
        this.repository = repository;
        this.tableDescriptors = toDescriptors(entities);
        this.config = config;
    }

    public void run() {
        TxManager txManager = new StdTxManager(repository);
        Stopwatch totalTime = Stopwatch.createStarted();
        Stream<TableDescriptor<?>> stream = tableDescriptors.stream();
        if (config.parallel) {
            stream = stream.parallel();
        }
        stream.forEach(descriptor -> checkTable(txManager, descriptor));
        log.info("[{}] Data compatibility checked successfully", totalTime);
    }

    private <E extends Entity<E>> void checkTable(TxManager txManager, TableDescriptor<E> descriptor) {
        ReadTableParams.ReadTableParamsBuilder<Entity.Id<E>> paramsBuilder = ReadTableParams.<Entity.Id<E>>builder();
        if (config.rowLimit > 0) {
            paramsBuilder.rowLimit(config.rowLimit);
            paramsBuilder.ordered();
        }
        ReadTableParams<Entity.Id<E>> params = paramsBuilder.timeout(config.timeout).build();

        log.info("Checking entities of {}", descriptor.tableName());
        Stopwatch sw = Stopwatch.createStarted();
        try {
            txManager.readOnly().noFirstLevelCache().run(() -> {
                RepositoryTransaction tx = Tx.Current.get().getRepositoryTransaction();

                long checkedCount = tx.table(descriptor).readTable(params).count();

                log.info("[{}] Checked {} entities of {}", sw, checkedCount, descriptor.tableName());
            });
        } catch (Exception e) {
            String message = format("[%s] Got exception while checking entities of %s: ", sw, descriptor.tableName());
            if (config.skipSchemaErrors && e instanceof SchemaException) {
                log.warn(message);
            } else {
                log.error(message);
                throw e;
            }
        }
    }

    private static List<TableDescriptor<?>> toDescriptors(List<Class<? extends Entity>> entities) {
        List<TableDescriptor<?>> descriptors = new ArrayList<>();
        entities.forEach(e -> descriptors.add(TableDescriptor.from(EntitySchema.of(e))));
        return descriptors;
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
