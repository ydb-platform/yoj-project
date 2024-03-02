package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.yandex.ydb.ValueProtos;
import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.StatusCode;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.query.Params;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.settings.BulkUpsertSettings;
import com.yandex.ydb.table.settings.CommitTxSettings;
import com.yandex.ydb.table.settings.ExecuteDataQuerySettings;
import com.yandex.ydb.table.settings.ExecuteScanQuerySettings;
import com.yandex.ydb.table.settings.ReadTableSettings;
import com.yandex.ydb.table.settings.RollbackTxSettings;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.ListValue;
import com.yandex.ydb.table.values.StructValue;
import com.yandex.ydb.table.values.TupleValue;
import io.grpc.Context;
import io.grpc.Deadline;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.db.cache.RepositoryCacheImpl;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionIsolationLevelException;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionScanException;
import tech.ydb.yoj.repository.db.exception.OptimisticLockException;
import tech.ydb.yoj.repository.db.exception.RepositoryException;
import tech.ydb.yoj.repository.db.exception.UnavailableException;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.ydb.bulk.BulkMapper;
import tech.ydb.yoj.repository.ydb.client.ResultSetConverter;
import tech.ydb.yoj.repository.ydb.client.YdbConverter;
import tech.ydb.yoj.repository.ydb.client.YdbValidator;
import tech.ydb.yoj.repository.ydb.exception.BadSessionException;
import tech.ydb.yoj.repository.ydb.exception.ResultTruncatedException;
import tech.ydb.yoj.repository.ydb.exception.UnexpectedException;
import tech.ydb.yoj.repository.ydb.exception.YdbComponentUnavailableException;
import tech.ydb.yoj.repository.ydb.exception.YdbOverloadedException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.merge.QueriesMerger;
import tech.ydb.yoj.repository.ydb.readtable.ReadTableMapper;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.repository.ydb.table.YdbTable;
import tech.ydb.yoj.util.lang.Interrupts;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Strings.emptyToNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.repository.ydb.client.YdbValidator.validatePkConstraint;
import static tech.ydb.yoj.repository.ydb.client.YdbValidator.validateTruncatedResults;

public class YdbRepositoryTransaction<REPO extends YdbRepository>
        implements BaseDb, RepositoryTransaction, YdbTable.QueryExecutor, TransactionLocal.Holder {
    private static final Logger log = LoggerFactory.getLogger(YdbRepositoryTransaction.class);

    private final List<YdbRepository.Query<?>> pendingWrites = new ArrayList<>();
    private final List<Stream<?>> openedStreams = new ArrayList<>();

    @Getter
    private final TxOptions options;
    @Getter
    private final TransactionLocal transactionLocal;
    private final RepositoryCache cache;

    protected final REPO repo;

    private Session session = null;
    private Stopwatch sessionSw;
    protected String txId = null;
    private String firstNonNullTxId = null; // used for logs
    private String closeAction = null; // used to detect of usage transaction after commit()/rollback()
    private boolean isBadSession = false;

    public YdbRepositoryTransaction(REPO repo, @NonNull TxOptions options) {
        this.repo = repo;
        this.options = options;
        this.transactionLocal = new TransactionLocal(options);
        this.cache = options.isFirstLevelCache() ? new RepositoryCacheImpl() : RepositoryCache.empty();
    }

    private <V> Stream<V> makeStream(YdbSpliterator<V> spliterator) {
        Stream<V> stream = spliterator.createStream();
        openedStreams.add(stream);
        return stream;
    }

    @Override
    public <T extends Entity<T>> Table<T> table(Class<T> c) {
        return new YdbTable<>(c, this);
    }

    @Override
    public void commit() {
        if (isBadSession) {
            throw new IllegalStateException("Transaction was invalidated. Commit isn't possible");
        }
        try {
            flushPendingWrites();
        } catch (Throwable t) {
            rollback();
            throw t;
        }
        endTransaction("commit", this::doCommit);
    }

    @Override
    public void rollback() {
        Interrupts.runInCleanupMode(() -> {
            try {
                endTransaction("rollback", () -> {
                    Status status = YdbOperations.safeJoin(session.rollbackTransaction(txId, new RollbackTxSettings()));
                    validate("rollback", status.getCode(), status.toString());
                });
            } catch (Throwable t) {
                log.info("Failed to rollback the transaction", t);
            }
        });
    }

    private void doCommit() {
        try {
            Status status = YdbOperations.safeJoin(session.commitTransaction(txId, new CommitTxSettings()));
            validatePkConstraint(status.getIssues());
            validate("commit", status.getCode(), status.toString());
        } catch (YdbComponentUnavailableException | YdbOverloadedException e) {
            throw new UnavailableException("Unknown transaction state: commit was sent, but result is unknown", e);
        }
    }

    private void closeStreams() {
        Exception summaryException = null;
        for (Stream<?> stream : openedStreams) {
            try {
                stream.close();
            } catch (Exception e) {
                if (summaryException == null) {
                    summaryException = e;
                } else {
                    summaryException.addSuppressed(e);
                }
            }
        }
        if (summaryException != null) {
            // Leak are possible because Spliterator thread can wait on writing to stream
            throw new UnexpectedException("Exceptions on stream close. Thread leak are possible", summaryException);
        }
    }

    private void validate(String request, StatusCode statusCode, String response) {
        try {
            YdbValidator.validate(request, statusCode, response);
        } catch (BadSessionException | OptimisticLockException e) {
            transactionLocal.log().info("Request got %s: DB tx was invalidated", e.getClass().getSimpleName());
            isBadSession = true;
            throw e;
        }
    }

    private boolean isFinalActionNeeded(String actionName) {
        if (session == null || isBadSession) {
            transactionLocal.log().info("No-op %s: no active DB session", actionName);
            return false;
        }
        if (options.isScan()) {
            transactionLocal.log().info("No-op %s: scan tx", actionName);
            return false;
        }
        if (options.isReadOnly()) {
            transactionLocal.log().info("No-op %s: read-only tx @%s", actionName, options.getIsolationLevel());
            return false;
        }
        if (txId == null) {
            transactionLocal.log().info("No-op %s: no active transaction in session", actionName);
            return false;
        }
        return true;
    }

    private void endTransaction(String actionName, Runnable finalAction) {
        try {
            closeStreams();

            if (isFinalActionNeeded(actionName)) {
                doCall(actionName, finalAction);
            }
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new UnexpectedException("Could not " + actionName + " " + txId, e);
        } finally {
            closeAction = actionName;
            if (session != null) {
                transactionLocal.log().info("[[%s]] TOTAL (txId=%s,sessionId=%s)", sessionSw, firstNonNullTxId, session.getId());
                // NB: We use getSessionManager() method to allow mocking YdbRepository
                repo.getSessionManager().release(session);
                session = null;
            }
        }
    }

    private TxControl<?> getTxControl() {
        return switch (options.getIsolationLevel()) {
            case SERIALIZABLE_READ_WRITE -> {
                TxControl<?> txControl = (txId != null ? TxControl.id(txId) : TxControl.serializableRw());
                yield txControl.setCommitTx(false);
            }
            case ONLINE_CONSISTENT_READ_ONLY -> TxControl.onlineRo().setAllowInconsistentReads(false);
            case ONLINE_INCONSISTENT_READ_ONLY -> TxControl.onlineRo().setAllowInconsistentReads(true);
            case STALE_CONSISTENT_READ_ONLY -> TxControl.staleRo();
        };
    }

    private String getYql(Statement<?, ?> statement) {
        return "--!syntax_v1\n" + statement.getQuery(repo.getTablespace());
    }

    private <PARAMS> Params getSdkParams(Statement<PARAMS, ?> statement, PARAMS params) {
        Map<String, ValueProtos.TypedValue> values = params == null ? Map.of() : statement.toQueryParameters(params);
        return YdbConverter.convertToParams(values);
    }

    private void flushPendingWrites() {
        transactionLocal.projectionCache().applyProjectionChanges(this);
        QueriesMerger.create(cache)
                .merge(pendingWrites)
                .forEach(this::execute);
    }

    @Override
    public <PARAMS, RESULT> List<RESULT> execute(Statement<PARAMS, RESULT> statement, PARAMS params) {
        List<RESULT> result = statement.readFromCache(params, cache);
        if (result != null) {
            String actionStr = statement.toDebugString(params);
            String resultStr = debugResult(result);
            transactionLocal.log().debug("[statement cache] %s -> %s", actionStr, resultStr);
            return result;
        }

        result = doCall(statement.toDebugString(params), () -> {
            if (options.isScan()) {
                if (options.getScanOptions().isUseNewSpliterator()) {
                    return doExecuteScanQueryList(statement, params);
                } else {
                    return doExecuteScanQueryLegacy(statement, params);
                }
            } else {
                return doExecuteDataQuery(statement, params);
            }
        });

        trace(statement, result);
        statement.storeToCache(params, result, cache);

        return result;
    }

    private <PARAMS, RESULT> List<RESULT> doExecuteDataQuery(Statement<PARAMS, RESULT> statement, PARAMS params) {
        String yql = getYql(statement);
        TxControl<?> txControl = getTxControl();
        Params sdkParams = getSdkParams(statement, params);
        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();
        if (statement.isPreparable()) {
            settings = settings.keepInQueryCache();
        }


        //TODO: remove grpc dependency from data access code
        Deadline grpcDeadline = Context.current().getDeadline();
        Duration grpcTimeout = null;
        if (grpcDeadline != null) {
            grpcTimeout = Duration.ofNanos(grpcDeadline.timeRemaining(TimeUnit.NANOSECONDS));
        }

        TxOptions.TimeoutOptions timeoutOptions = options.minTimeoutOptions(grpcTimeout);
        settings.setDeadlineAfter(timeoutOptions.getDeadlineAfter());
        settings.setCancelAfter(timeoutOptions.getCancelAfter());

        // todo
        // settings.setTraceId();

        Result<DataQueryResult> result = YdbOperations.safeJoin(session.executeDataQuery(yql, txControl, sdkParams, settings));

        result.ok().ifPresent(queryResult -> {
            txId = emptyToNull(queryResult.getTxId());
            if (firstNonNullTxId == null) {
                firstNonNullTxId = txId;
            }
        });

        validatePkConstraint(result.getIssues());
        validate(yql, result.getCode(), result.toString());

        DataQueryResult queryResult = result.expect("expect result after sql execution");
        if (queryResult.getResultSetCount() > 1) {
            throw new YdbRepositoryException("Multi-table queries are not supported", yql, queryResult);
        }
        if (queryResult.getResultSetCount() == 0) {
            return null;
        }
        validateTruncatedResults(yql, queryResult);

        ResultSetReader resultSet = queryResult.getResultSet(0);
        return new ResultSetConverter(resultSet).stream(statement::readResult).collect(toList());
    }

    private <PARAMS, RESULT> List<RESULT> doExecuteScanQueryLegacy(Statement<PARAMS, RESULT> statement, PARAMS params) {
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .timeout(options.getScanOptions().getTimeout())
                .mode(com.yandex.ydb.table.YdbTable.ExecuteScanQueryRequest.Mode.MODE_EXEC)
                .build();

        String yql = getYql(statement);
        Params sdkParams = getSdkParams(statement, params);

        List<RESULT> result = new ArrayList<>();
        Status status = YdbOperations.safeJoin(session.executeScanQuery(yql, sdkParams, settings, rs -> {
            if (result.size() + rs.getRowCount() > options.getScanOptions().getMaxSize()) {
                throw new ResultTruncatedException(
                        format("Query result size became greater than %d", options.getScanOptions().getMaxSize()),
                        yql, result.size()
                );
            }
            new ResultSetConverter(rs).stream(statement::readResult).forEach(result::add);
        }));

        validate("SCAN_QUERY: " + yql, status.getCode(), status.toString());

        return result;
    }

    private <PARAMS, RESULT> List<RESULT> doExecuteScanQueryList(Statement<PARAMS, RESULT> statement, PARAMS params) {
        List<RESULT> result = new ArrayList<>();
        try (Stream<RESULT> stream = executeScanQuery(statement, params)) {
            stream.forEach(r -> {
                if (result.size() >= options.getScanOptions().getMaxSize()) {
                    throw new ResultTruncatedException(
                            format("Query result size became greater than %d", options.getScanOptions().getMaxSize()),
                            getYql(statement), result.size()
                    );
                }
                result.add(r);
            });
        }
        return result;
    }

    @Override
    public <PARAMS, RESULT> Stream<RESULT> executeScanQuery(Statement<PARAMS, RESULT> statement, PARAMS params) {
        if (!options.isScan()) {
            throw new IllegalStateException("Scan query can be used only from scan tx");
        }
        
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .timeout(options.getScanOptions().getTimeout())
                .mode(com.yandex.ydb.table.YdbTable.ExecuteScanQueryRequest.Mode.MODE_EXEC)
                .build();

        String yql = getYql(statement);
        Params sdkParams = getSdkParams(statement, params);

        YdbSpliterator<RESULT> spliterator = new YdbSpliterator<>("scanQuery: " + yql, false);

        initSession();
        session.executeScanQuery(
                yql, sdkParams, settings,
                rs -> new ResultSetConverter(rs).stream(statement::readResult).forEach(spliterator::onNext)
        ).whenComplete(spliterator::onSupplierThreadComplete);

        return makeStream(spliterator);
    }

    @Override
    public <PARAMS> void pendingExecute(Statement<PARAMS, ?> statement, PARAMS value) {
        if (options.isScan()) {
            throw new IllegalTransactionScanException("Mutable operations");
        }
        if (options.isReadOnly()) {
            throw new IllegalTransactionIsolationLevelException("Mutable operations", options.getIsolationLevel());
        }
        YdbRepository.Query<PARAMS> query = new YdbRepository.Query<>(statement, value);
        if (options.isImmediateWrites()) {
            execute(query);
            transactionLocal.projectionCache().applyProjectionChanges(this);
        } else {
            pendingWrites.add(query);
        }
    }

    @SuppressWarnings("unchecked")
    private <PARAMS> void execute(YdbRepository.Query<PARAMS> query) {
        if (query.getValues().size() == 1) {
            execute(query.getStatement(), query.getValues().get(0));
        } else {
            execute(query.getStatement(), (PARAMS) query.getValues());
        }
    }

    @Override
    public <IN> void bulkUpsert(BulkMapper<IN> mapper, List<IN> input, BulkParams params) {
        String tableName = mapper.getTableName(repo.getTablespace());

        doCall("bulk upsert to table " + mapper.getTableName(""), () -> {
            var values = input.stream().map(x -> StructValue.of(
                            mapper.map(x).entrySet().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Map.Entry::getKey,
                                                    entry -> YdbConverter.toSDK(entry.getValue())
                                            )
                                    )
                    )
            ).toArray(com.yandex.ydb.table.values.Value[]::new);

            var settings = new BulkUpsertSettings();
            settings.setTimeout(params.getTimeout());
            settings.setCancelAfter(params.getCancelAfter());
            settings.setDeadlineAfter(params.getDeadlineAfter());
            settings.setTraceId(params.getTraceId());

            try {
                Status status = YdbOperations.safeJoin(
                        session.executeBulkUpsert(
                                tableName,
                                ListValue.of(values),
                                settings
                        )
                );
                validate("bulkInsert", status.getCode(), status.toString());
            } catch (RepositoryException e) {
                throw e;
            } catch (Exception e) {
                throw new UnexpectedException("Could not bulk insert into table " + tableName, e);
            }
        });
    }

    @Override
    public <PARAMS, RESULT> Stream<RESULT> readTable(ReadTableMapper<PARAMS, RESULT> mapper, ReadTableParams<PARAMS> params) throws RepositoryException {
        if (options.isReadWrite()) {
            throw new IllegalTransactionIsolationLevelException("readTable", options.getIsolationLevel());
        }
        String tableName = mapper.getTableName(repo.getTablespace());
        ReadTableSettings.Builder settings = ReadTableSettings.newBuilder()
                .orderedRead(params.isOrdered())
                .timeout(params.getTimeout())
                .rowLimit(params.getRowLimit())
                .columns(mapper.getColumns());
        if (params.getFromKey() != null) {
            var values = mapper.mapKey(params.getFromKey()).stream()
                    .map(typedValue -> YdbConverter.toSDK(typedValue.getType(), typedValue.getValue()))
                    .collect(toList());
            settings.fromKey(TupleValue.of(values), params.isFromInclusive());
        }
        if (params.getToKey() != null) {
            var values = mapper.mapKey(params.getToKey()).stream()
                    .map(typedValue -> YdbConverter.toSDK(typedValue.getType(), typedValue.getValue()))
                    .collect(toList());
            settings.toKey(TupleValue.of(values), params.isToInclusive());
        }

        if (params.isUseNewSpliterator()) {
            YdbSpliterator<RESULT> spliterator = new YdbSpliterator<>("readTable: " + tableName, params.isOrdered());

            initSession();
            session.readTable(
                    tableName, settings.build(),
                    resultSet -> new ResultSetConverter(resultSet).stream(mapper::mapResult).forEach(spliterator::onNext)
            ).whenComplete(spliterator::onSupplierThreadComplete);

            return makeStream(spliterator);
        }

        try {
            YdbLegacySpliterator<RESULT> spliterator = new YdbLegacySpliterator<>(params.isOrdered(), action ->
                    doCall("read table " + mapper.getTableName(""), () -> {
                        Status status = YdbOperations.safeJoin(
                                session.readTable(
                                        tableName,
                                        settings.build(),
                                        rs -> new ResultSetConverter(rs).stream(mapper::mapResult).forEach(action)
                                ),
                                params.getTimeout().plusMinutes(5)
                        );
                        validate("readTable", status.getCode(), status.toString());
                    })
            );
            return spliterator.makeStream();
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new UnexpectedException("Could not read table " + tableName, e);
        }
    }

    private void doCall(String actionStr, Runnable call) {
        doCall(actionStr, () -> {
            call.run();
            return null;
        });
    }

    private void initSession() {
        if (closeAction != null) {
            throw new IllegalStateException("Transaction already closed by " + closeAction);
        }
        if (session == null) {
            // NB: We use getSessionManager() method to allow mocking YdbRepository
            session = repo.getSessionManager().getSession();
            sessionSw = Stopwatch.createStarted();
        }
    }

    private <R> R doCall(String actionStr, Supplier<R> call) {
        initSession();

        Stopwatch sw = Stopwatch.createStarted();
        String resultStr = "";
        try {
            R result = call.get();
            resultStr = (result == null ? "" : " -> " + debugResult(result));
            return result;
        } catch (Exception e) {
            resultStr = " => " + e.getClass().getName();
            throw e;
        } finally {
            transactionLocal.log().debug("[ %s ] %s", sw, actionStr + resultStr);
        }
    }

    private String debugResult(Object result) {
        if (result instanceof Iterable) {
            int size = Iterables.size((Iterable<?>) result);
            return size == 1 ? String.valueOf(((Iterable<?>) result).iterator().next()) : "[" + size + "]";
        } else {
            return String.valueOf(result);
        }
    }

    private void trace(@NonNull Statement<?, ?> statement, Object results) {
        log.trace("{}", new Object() {
            @Override
            public String toString() {
                return format("[txId=%s,sessionId=%s] %s%s", firstNonNullTxId, session.getId(), statement, debugResult(results));
            }
        });
    }
}
