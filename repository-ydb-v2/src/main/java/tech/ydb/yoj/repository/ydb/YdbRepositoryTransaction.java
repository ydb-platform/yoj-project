package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import io.grpc.Context;
import io.grpc.Deadline;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.common.transaction.YdbTransaction;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.ValueProtos;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.query.stats.CompilationStats;
import tech.ydb.table.query.stats.OperationStats;
import tech.ydb.table.query.stats.QueryPhaseStats;
import tech.ydb.table.query.stats.QueryStats;
import tech.ydb.table.query.stats.QueryStatsCollectionMode;
import tech.ydb.table.query.stats.TableAccessStats;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.BaseDb;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.QueryStatsMode;
import tech.ydb.yoj.repository.db.QueryTracingFilter;
import tech.ydb.yoj.repository.db.QueryType;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.TxOptions;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
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
import tech.ydb.yoj.util.lang.Strings;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Strings.emptyToNull;
import static java.lang.Boolean.getBoolean;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.ydb.client.YdbValidator.validatePkConstraint;

public class YdbRepositoryTransaction<REPO extends YdbRepository>
        implements BaseDb, RepositoryTransaction, YdbTable.QueryExecutor {
    private static final Logger log = LoggerFactory.getLogger(YdbRepositoryTransaction.class);

    private static final String PROP_TRACE_DUMP_YDB_PARAMS = "tech.ydb.yoj.repository.ydb.trace.dumpYdbParams";
    private static final String PROP_TRACE_VERBOSE_OBJ_PARAMS = "tech.ydb.yoj.repository.ydb.trace.verboseObjParams";
    private static final String PROP_TRACE_VERBOSE_OBJ_RESULTS = "tech.ydb.yoj.repository.ydb.trace.verboseObjResults";

    private final List<YdbRepository.Query<?>> pendingWrites = new ArrayList<>();
    private final List<YdbSpliterator<?>> spliterators = new ArrayList<>();

    @Getter
    private final TxOptions options;
    @Getter
    private final TransactionLocal transactionLocal;
    private final RepositoryCache cache;
    private final String tablespace;

    protected final REPO repo;

    private Session session = null;
    private Stopwatch sessionSw;
    protected String txId = null;
    private String firstNonNullTxId = null; // used for logs
    private String closeAction = null; // used to detect of usage transaction after commit()/rollback()
    private boolean isBadSession = false;

    public YdbRepositoryTransaction(REPO repo, TxOptions options) {
        this.repo = repo;
        this.options = options;
        this.transactionLocal = new TransactionLocal(options);
        this.cache = options.isFirstLevelCache() ? RepositoryCache.create() : RepositoryCache.empty();
        this.tablespace = repo.getSchemaOperations().getTablespace();
    }

    private <V> YdbSpliterator<V> createSpliterator(String request, boolean isOrdered) {
        YdbSpliterator<V> spliterator = new YdbSpliterator<>(request, isOrdered);
        spliterators.add(spliterator);
        return spliterator;
    }

    @Override
    public <T extends Entity<T>> Table<T> table(Class<T> c) {
        return new YdbTable<>(c, this);
    }

    @Override
    public <T extends Entity<T>> Table<T> table(TableDescriptor<T> tableDescriptor) {
        return new YdbTable<>(tableDescriptor, this);
    }

    @Override
    public void commit() {
        if (isBadSession) {
            log.error("Transaction was invalidated, but exception was omitted. Commit must not be called after error");
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
        for (YdbSpliterator<?> spliterator : spliterators) {
            try {
                spliterator.close();
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
        if (!isBadSession) {
            isBadSession = YdbValidator.isTransactionClosedByServer(statusCode);
        }
        try {
            YdbValidator.validate(request, statusCode, response);
        } catch (BadSessionException | OptimisticLockException e) {
            transactionLocal.log().info("Request got %s: DB tx was invalidated", e.getClass().getSimpleName());
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
        if (options.isReadOnly() && options.getIsolationLevel() != IsolationLevel.SNAPSHOT) {
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
                session.close();
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
            case SNAPSHOT -> {
                TxControl<?> txControl = (txId != null ? TxControl.id(txId) : TxControl.snapshotRo());
                yield txControl.setCommitTx(false);
            }
        };
    }

    private String getYql(Statement<?, ?> statement) {
        // TODO(nvamelichev): Make the use of syntax_v1 directive configurable in YdbRepository.Settings
        // @see https://github.com/ydb-platform/yoj-project/issues/148
        return statement.getQuery(tablespace);
    }

    private static <PARAMS> Params getSdkParams(Statement<PARAMS, ?> statement, PARAMS params) {
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

        Exception thrown = null;
        try {
            result = doCall(statement.toDebugString(params), () -> {
                if (options.isScan()) {
                    return options.getScanOptions().isUseNewSpliterator()
                            ? doExecuteScanQueryList(statement, params)
                            : doExecuteScanQueryLegacy(statement, params);
                } else {
                    return doExecuteDataQuery(statement, params);
                }
            });
        } catch (Exception e) {
            thrown = e;
            throw e;
        } finally {
            trace(statement, params, thrown, result);
        }

        statement.storeToCache(params, result, cache);

        return result;
    }

    private <PARAMS, RESULT> List<RESULT> doExecuteDataQuery(Statement<PARAMS, RESULT> statement, PARAMS params) {
        String yql = getYql(statement);
        TxControl<?> txControl = getTxControl();
        Params sdkParams = getSdkParams(statement, params);
        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();
        if (!statement.isPreparable()) {
            settings.disableQueryCache();
        }

        //TODO: remove grpc dependency from data access code
        Deadline grpcDeadline = Context.current().getDeadline();
        Duration grpcTimeout = null;
        if (grpcDeadline != null) {
            grpcTimeout = Duration.ofNanos(grpcDeadline.timeRemaining(TimeUnit.NANOSECONDS));
        }

        TxOptions.TimeoutOptions timeoutOptions = options.minTimeoutOptions(grpcTimeout);
        settings.setTimeout(timeoutOptions.getTimeout());
        settings.setCancelAfter(timeoutOptions.getCancelAfter());

        settings.setCollectStats(getSdkStatsMode());

        // todo
        // settings.setTraceId();

        Result<DataQueryResult> result = YdbOperations.safeJoin(session.executeDataQuery(yql, txControl, sdkParams, settings));

        if (result.isSuccess()) {
            txId = emptyToNull(result.getValue().getTxId());
            if (firstNonNullTxId == null) {
                firstNonNullTxId = txId;
            }
        }

        validatePkConstraint(result.getStatus().getIssues());
        validate(yql, result.getStatus().getCode(), result.toString());

        DataQueryResult queryResult = result.getValue();
        if (queryResult.getResultSetCount() > 1) {
            throw new YdbRepositoryException("Multi-table queries are not supported", yql, queryResult);
        }
        if (queryResult.getResultSetCount() == 0) {
            return null;
        }
        validateTruncatedResults(yql, queryResult);

        QueryStats queryStats = queryResult.getQueryStats();
        if (queryStats != null) {
            transactionLocal.log().debug(() -> logQueryStats(queryStats));
        }

        ResultSetReader resultSet = queryResult.getResultSet(0);
        return new ResultSetConverter(resultSet).stream(statement::readResult).collect(toList());
    }

    private void validateTruncatedResults(String yql, DataQueryResult queryResult) {
        for (int i = 0; i < queryResult.getResultSetCount(); i++) {
            ResultSetReader rs = queryResult.getResultSet(i);
            int rowCount = rs.getRowCount();
            if (rs.isTruncated()) {
                throw new ResultTruncatedException(
                        "Query results were truncated to " + rowCount + " elements; please specify a LIMIT",
                        yql,
                        rowCount,
                        rowCount
                );
            }
        }
    }

    private <PARAMS, RESULT> List<RESULT> doExecuteScanQueryLegacy(Statement<PARAMS, RESULT> statement, PARAMS params) {
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(options.getScanOptions().getTimeout())
                .setMode(ExecuteScanQuerySettings.Mode.EXEC)
                .build();

        String yql = getYql(statement);
        Params sdkParams = getSdkParams(statement, params);

        List<RESULT> result = new ArrayList<>();
        Status status = YdbOperations.safeJoin(session.executeScanQuery(yql, sdkParams, settings, rs -> {
            int rowCount = result.size() + rs.getRowCount();
            if (rowCount > options.getScanOptions().getMaxSize()) {
                throw new ResultTruncatedException(
                        "Scan query result size became greater than " + options.getScanOptions().getMaxSize(),
                        yql,
                        options.getScanOptions().getMaxSize(),
                        rowCount
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
                            "Scan query result size became greater than " + options.getScanOptions().getMaxSize(),
                            getYql(statement),
                            options.getScanOptions().getMaxSize(),
                            result.size()
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
                .withRequestTimeout(options.getScanOptions().getTimeout())
                .setMode(ExecuteScanQuerySettings.Mode.EXEC)
                .build();

        String yql = getYql(statement);
        Params sdkParams = getSdkParams(statement, params);

        YdbSpliterator<RESULT> spliterator = createSpliterator("scanQuery: " + yql, false);

        initSession();
        session.executeScanQuery(
                yql, sdkParams, settings,
                rs -> new ResultSetConverter(rs).stream(statement::readResult).forEach(spliterator::onNext)
        ).whenComplete(spliterator::onSupplierThreadComplete);

        return spliterator.createStream();
    }

    private QueryStatsCollectionMode getSdkStatsMode() {
        var queryStats = options.getQueryStats();
        return queryStats == null
                ? QueryStatsCollectionMode.NONE
                : QueryStatsCollectionMode.valueOf(queryStats.name());
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
        String tableName = mapper.getTableName(tablespace);

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
            ).toArray(Value[]::new);

            var settings = new BulkUpsertSettings();
            settings.setTimeout(params.getTimeout());
            settings.setCancelAfter(params.getCancelAfter());
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
        String tableName = mapper.getTableName(tablespace);
        ReadTableSettings.Builder settings = ReadTableSettings.newBuilder()
                .orderedRead(params.isOrdered())
                .withRequestTimeout(params.getTimeout())
                .rowLimit(params.getRowLimit())
                .columns(mapper.getColumns())
                .batchLimitBytes(params.getBatchLimitBytes())
                .batchLimitRows(params.getBatchLimitRows());
        if (params.getFromKey() != null) {
            List<Value<?>> values = mapper.mapKey(params.getFromKey()).stream()
                    .map(typedValue -> YdbConverter.toSDK(typedValue.getType(), typedValue.getValue()))
                    .collect(toList());
            settings.fromKey(TupleValue.of(values), params.isFromInclusive());
        }
        if (params.getToKey() != null) {
            List<Value<?>> values = mapper.mapKey(params.getToKey()).stream()
                    .map(typedValue -> YdbConverter.toSDK(typedValue.getType(), typedValue.getValue()))
                    .collect(toList());
            settings.toKey(TupleValue.of(values), params.isToInclusive());
        }

        if (params.isUseNewSpliterator()) {
            YdbSpliterator<RESULT> spliterator = createSpliterator("readTable: " + tableName, params.isOrdered());

            initSession();
            session.readTable(
                    tableName, settings.build(),
                    resultSet -> new ResultSetConverter(resultSet).stream(mapper::mapResult).forEach(spliterator::onNext)
            ).whenComplete(spliterator::onSupplierThreadComplete);

            return spliterator.createStream();
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

    /**
     * @return YDB SDK {@link YdbTransaction} wrapping this {@code YdbRepositoryTransaction}
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/80")
    public YdbTransaction toSdkTransaction() {
        return new YdbTransaction() {
            @Nullable
            @Override
            public String getId() {
                return txId;
            }

            @Override
            public TxMode getTxMode() {
                return switch (options.getIsolationLevel()) {
                    case SERIALIZABLE_READ_WRITE -> TxMode.SERIALIZABLE_RW;
                    case ONLINE_CONSISTENT_READ_ONLY -> TxMode.ONLINE_RO;
                    case ONLINE_INCONSISTENT_READ_ONLY -> TxMode.ONLINE_INCONSISTENT_RO;
                    case STALE_CONSISTENT_READ_ONLY -> TxMode.STALE_RO;
                    case SNAPSHOT -> TxMode.SNAPSHOT_RO;
                    // TxMode.NONE corresponds to DDL statements, and we have no DDL statements in YOJ transactions
                };
            }

            @Override
            public String getSessionId() {
                Preconditions.checkState(!isBadSession, "No active YDB session (tx closed by YDB side)");
                Preconditions.checkState(session != null, "No active YDB session");
                return session.getId();
            }

            @Override
            public CompletableFuture<Status> getStatusFuture() {
                throw new UnsupportedOperationException();
            }
        };
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

    private static String debugResult(Object result) {
        if (result instanceof Iterable) {
            int size = Iterables.size((Iterable<?>) result);
            return size == 1 ? String.valueOf(((Iterable<?>) result).iterator().next()) : "[" + size + "]";
        } else {
            return String.valueOf(result);
        }
    }

    private void trace(@NonNull Statement<?, ?> statement, Object params, Throwable thrown, Object results) {
        var tracingFilter = options.getTracingFilter();
        if (!shouldTrace(tracingFilter, statement, thrown)) {
            return;
        }

        var txId = firstNonNullTxId;
        var sessionId = session == null ? null : session.getId();
        log.trace("{}", new StatementTraceEvent(
                statement,
                txId, sessionId, tablespace,
                params, thrown, results
        ));
    }

    private boolean shouldTrace(@Nullable QueryTracingFilter tracingFilter, @NonNull Statement<?, ?> statement, Throwable thrown) {
        if (tracingFilter == null || tracingFilter == QueryTracingFilter.ENABLE_ALL) {
            return true;
        }
        if (tracingFilter == QueryTracingFilter.DISABLE_ALL) {
            return false;
        }

        // NB: we have to return txName = "???" for a RepositoryTransaction has been created not by TxManager+Tx,
        // but manually by calling Repository.startTransaction() (and thus has no Tx.Current thread-local value).
        // I (@nvamelichev) know of NO production code that uses that low-level YOJ API directly, except for YOJ tests.
        var txName = Tx.Current.exists() ? Tx.Current.get().getName() : "???";

        var queryType = switch (statement.getQueryType()) {
            case UNTYPED -> QueryType.GENERIC;
            case SELECT -> QueryType.FIND;
            case INSERT -> QueryType.INSERT;
            case UPSERT -> QueryType.SAVE;
            case UPDATE -> QueryType.UPDATE;
            case DELETE, DELETE_ALL -> QueryType.DELETE;
        };

        return tracingFilter.shouldTrace(txName, options, queryType, thrown);
    }

    private List<?> logQueryStats(QueryStats queryStats) {
        List<String> logLines = new ArrayList<>();
        logLines.add("| Query: "
                + "Duration " + queryStats.getTotalDurationUs() + " us, "
                + "CPU Time " + queryStats.getTotalCpuTimeUs() + " us, "
                + "Process CPU Time " + queryStats.getProcessCpuTimeUs() + " us; "
                + queryStats.getQueryPhasesCount() + " Query Phase(s)");

        var compilationStats = queryStats.getCompilation();
        if (compilationStats != null) {
            logLines.add("|___ Compilation: " + compilationStatsToString(compilationStats));
        }

        boolean includeQueryPlan = options.getQueryStats().compareTo(QueryStatsMode.FULL) >= 0;
        boolean includeQueryAst = options.getQueryStats().compareTo(QueryStatsMode.PROFILE) >= 0;

        if (queryStats.getQueryPhasesCount() > 0) {
            var phaseIter = queryStats.getQueryPhasesList().iterator();
            while (phaseIter.hasNext()) {
                var phaseStats = phaseIter.next();
                var hasMoreLines = phaseIter.hasNext() || includeQueryPlan /* || includeQueryAst */;
                logQueryPhaseTo(logLines, phaseStats, hasMoreLines);
            }
        }

        if (includeQueryPlan) {
            logLines.add("|___ Query Plan: " + queryStats.getQueryPlan());
        }
        if (includeQueryAst) {
            logLines.add("|___ Query AST: " + Strings.removeSuffix(queryStats.getQueryAst(), "\n"));
        }

        return logLines;
    }

    private void logQueryPhaseTo(List<String> logLines, QueryPhaseStats phaseStats, boolean hasMoreLines) {
        logLines.add("|___ Query Phase: "
                + "Duration " + phaseStats.getDurationUs() + " us, "
                + "CPU Time " + phaseStats.getCpuTimeUs() + " us; "
                + (phaseStats.getLiteralPhase() ? "Literal" : "NOT Literal") + "; "
                + phaseStats.getAffectedShards() + " Shard(s) Affected; "
                + phaseStats.getTableAccessCount() + " Table Access(es)"
        );

        if (phaseStats.getTableAccessCount() > 0) {
            for (TableAccessStats tableStats : phaseStats.getTableAccessList()) {
                logTableAccessTo(logLines, tableStats, hasMoreLines);
            }
        }
    }

    private void logTableAccessTo(List<String> logLines, TableAccessStats tableStats, boolean hasMoreLines) {
        logLines.add((hasMoreLines ? "|" : " ") + "   |___ Table Access: `" + tableStats.getName() + "`; "
                + "Partitions: " + tableStats.getPartitionsCount() + "; "
                + "Read: " + tableOperationToString(tableStats.getReads()) + "; "
                + "Updated: " + tableOperationToString(tableStats.getUpdates()) + "; "
                + "Deleted: " + tableOperationToString(tableStats.getDeletes())
        );
    }

    private static String compilationStatsToString(CompilationStats compilationStats) {
        return (compilationStats.getFromCache() ? "From Cache" : "NOT From Cache") + ", "
                + "Duration " + compilationStats.getDurationUs() + " us, "
                + "CPU Time " + compilationStats.getCpuTimeUs() + " us";
    }

    private static String tableOperationToString(OperationStats op) {
        return op == null ? "---" : op.getRows() + " Row(s), " + op.getBytes() + " Byte(s)";
    }

    @RequiredArgsConstructor(access = PRIVATE)
    private static final class StatementTraceEvent {
        @NonNull
        private final Statement<?, ?> statement;

        private final String txId;
        private final String sessionId;

        private final String tablespace;

        private final Object params;
        private final Throwable thrown;
        private final Object results;

        @Override
        public String toString() {
            return (thrown != null ? "Failed" : "Successful") + " query: " + statement.getQueryType() + " [" + statementShortName(statement) + "] "
                    + "in (txId=" + txId + ",sessionId=" + sessionId + "):\n"
                    + statement.getQuery(com.google.common.base.Strings.nullToEmpty(tablespace))
                    + statementParams(statement, params)
                    + statementResults(thrown, results);
        }

        private String statementShortName(Statement<?, ?> statement) {
            var stmtClass = statement.getClass();

            if (stmtClass.isAnonymousClass() || stmtClass.isLocalClass() || stmtClass.isHidden()) {
                return stmtClass.getName();
            }

            if (stmtClass.getPackage().getName().startsWith(getClass().getPackageName() + ".")) {
                return stmtClass.getSimpleName();
            } else {
                return stmtClass.getCanonicalName();
            }
        }

        private static String statementParams(@NonNull Statement<?, ?> statement, Object params) {
            if (params == null) {
                return "";
            }

            var sb = new StringBuilder();
            sb.append('\n');
            sb.append("--  IN OBJ <- ");
            sb.append(getBoolean(PROP_TRACE_VERBOSE_OBJ_PARAMS) ? params : debugResult(params));

            if (getBoolean(PROP_TRACE_DUMP_YDB_PARAMS)) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Map<String, Value<?>> ydbParams = getSdkParams((Statement) statement, params).values();

                if (!ydbParams.isEmpty()) {
                    var iter = ydbParams.entrySet().iterator();
                    sb.append('\n');
                    while (iter.hasNext()) {
                        var entry = iter.next();
                        sb.append("--  IN YDB <- ");
                        sb.append(entry.getKey()).append(" = ").append(entry.getValue());
                        if (iter.hasNext()) {
                            sb.append('\n');
                        }
                    }
                }
            }

            return sb.toString();
        }

        private static String statementResults(Throwable thrown, Object results) {
            if (thrown != null) {
                return "\n-- OUT EXC => " + thrown.getClass().getName();
            } else if (results != null) {
                return "\n-- OUT OBJ -> " + (getBoolean(PROP_TRACE_VERBOSE_OBJ_RESULTS) ? results : debugResult(results));
            } else {
                return "";
            }
        }
    }
}
