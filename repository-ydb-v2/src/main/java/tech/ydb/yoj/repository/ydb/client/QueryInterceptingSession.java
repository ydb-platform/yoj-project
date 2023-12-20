package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.BeginTxSettings;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.CopyTableSettings;
import tech.ydb.table.settings.CopyTablesSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.DropTableSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.KeepAliveSessionSettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Calls {@link QueryInterceptor} before {@link #executeScanQuery(String, Params, ExecuteScanQuerySettings, Consumer)}
 * and {@link #executeDataQuery(String, TxControl, Params, ExecuteDataQuerySettings)}
 * then delegates to underlying {@link Session}
 */
public final class QueryInterceptingSession implements Session {
    private final Session delegate;
    private final QueryInterceptor interceptor;


    private QueryInterceptingSession(Session delegate, QueryInterceptor interceptor) {
        this.delegate = delegate;
        this.interceptor = interceptor;
    }

    public static Function<Session, Session> makeWrapper(QueryInterceptor interceptor) {
        return s -> new QueryInterceptingSession(s, interceptor);
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public CompletableFuture<Status> createTable(String path, TableDescription tableDescriptions, CreateTableSettings settings) {
        return delegate.createTable(path, tableDescriptions, settings);
    }

    @Override
    public CompletableFuture<Status> dropTable(String path, DropTableSettings settings) {
        return delegate.dropTable(path, settings);
    }

    @Override
    public CompletableFuture<Status> alterTable(String path, AlterTableSettings settings) {
        return delegate.alterTable(path, settings);
    }

    @Override
    public CompletableFuture<Status> copyTable(String src, String dst, CopyTableSettings settings) {
        return delegate.copyTable(src, dst, settings);
    }

    @Override
    public CompletableFuture<Status> copyTables(CopyTablesSettings copyTablesSettings) {
        return delegate.copyTables(copyTablesSettings);
    }

    @Override
    public CompletableFuture<Result<TableDescription>> describeTable(String path, DescribeTableSettings settings) {
        return delegate.describeTable(path, settings);
    }

    @Override
    public CompletableFuture<Result<DataQueryResult>> executeDataQuery(String query, TxControl txControl, Params params, ExecuteDataQuerySettings settings) {
        interceptor.beforeExecute(QueryType.DATA_QUERY, delegate, query);
        return delegate.executeDataQuery(query, txControl, params, settings);
    }

    @Override
    public CompletableFuture<Result<DataQuery>> prepareDataQuery(String query, PrepareDataQuerySettings settings) {
        return delegate.prepareDataQuery(query, settings);
    }

    @Override
    public CompletableFuture<Status> executeSchemeQuery(String query, ExecuteSchemeQuerySettings settings) {
        return delegate.executeSchemeQuery(query, settings);
    }

    @Override
    public CompletableFuture<Result<ExplainDataQueryResult>> explainDataQuery(String query, ExplainDataQuerySettings settings) {
        return delegate.explainDataQuery(query, settings);
    }

    @Override
    public CompletableFuture<Result<Transaction>> beginTransaction(Transaction.Mode transactionMode, BeginTxSettings settings) {
        return delegate.beginTransaction(transactionMode, settings);
    }

    @Override
    public CompletableFuture<Status> commitTransaction(String txId, CommitTxSettings settings) {
        return delegate.commitTransaction(txId, settings);
    }

    @Override
    public CompletableFuture<Status> rollbackTransaction(String txId, RollbackTxSettings settings) {
        return delegate.rollbackTransaction(txId, settings);
    }

    @Override
    public GrpcReadStream<ReadTablePart> executeReadTable(String s, ReadTableSettings readTableSettings) {
        return delegate.executeReadTable(s, readTableSettings);
    }

    @Override
    public GrpcReadStream<ResultSetReader> executeScanQuery(String s, Params params, ExecuteScanQuerySettings executeScanQuerySettings) {
        return delegate.executeScanQuery(s, params, executeScanQuerySettings);
    }

    @Override
    public CompletableFuture<Status> readTable(String tablePath, ReadTableSettings settings, Consumer<ResultSetReader> fn) {
        return delegate.readTable(tablePath, settings, fn);
    }

    @Override
    public CompletableFuture<Status> executeScanQuery(String query, Params params, ExecuteScanQuerySettings settings, Consumer<ResultSetReader> fn) {
        interceptor.beforeExecute(QueryType.SCAN_QUERY, delegate, query);
        return delegate.executeScanQuery(query, params, settings, fn);
    }

    @Override
    public CompletableFuture<Result<State>> keepAlive(KeepAliveSessionSettings settings) {
        return delegate.keepAlive(settings);
    }

    @Override
    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue rows, BulkUpsertSettings settings) {
        return delegate.executeBulkUpsert(tablePath, rows, settings);
    }

    @Override
    public void close() {
        delegate.close();
    }

    public enum QueryType {
        DATA_QUERY,
        SCAN_QUERY,
    }
}
