package tech.ydb.yoj.repository.ydb.client;

import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.SessionStatus;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.query.DataQuery;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.query.ExplainDataQueryResult;
import com.yandex.ydb.table.query.Params;
import com.yandex.ydb.table.result.ReadTableMeta;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.settings.AlterTableSettings;
import com.yandex.ydb.table.settings.BeginTxSettings;
import com.yandex.ydb.table.settings.BulkUpsertSettings;
import com.yandex.ydb.table.settings.CloseSessionSettings;
import com.yandex.ydb.table.settings.CommitTxSettings;
import com.yandex.ydb.table.settings.CopyTableSettings;
import com.yandex.ydb.table.settings.CreateTableSettings;
import com.yandex.ydb.table.settings.DescribeTableSettings;
import com.yandex.ydb.table.settings.DropTableSettings;
import com.yandex.ydb.table.settings.ExecuteDataQuerySettings;
import com.yandex.ydb.table.settings.ExecuteScanQuerySettings;
import com.yandex.ydb.table.settings.ExecuteSchemeQuerySettings;
import com.yandex.ydb.table.settings.ExplainDataQuerySettings;
import com.yandex.ydb.table.settings.KeepAliveSessionSettings;
import com.yandex.ydb.table.settings.PrepareDataQuerySettings;
import com.yandex.ydb.table.settings.ReadTableSettings;
import com.yandex.ydb.table.settings.RollbackTxSettings;
import com.yandex.ydb.table.transaction.Transaction;
import com.yandex.ydb.table.transaction.TransactionMode;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.ListValue;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Calls @link QueryInterceptor} before {@link #executeScanQuery(String, Params, ExecuteScanQuerySettings, Consumer)}
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
    public CompletableFuture<Result<Transaction>> beginTransaction(TransactionMode transactionMode, BeginTxSettings settings) {
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
    public CompletableFuture<Status> readTable(String tablePath, ReadTableSettings settings, BiConsumer<ResultSetReader, ReadTableMeta> fn) {
        return delegate.readTable(tablePath, settings, fn);
    }

    @Override
    public CompletableFuture<Status> executeScanQuery(String query, Params params, ExecuteScanQuerySettings settings, Consumer<ResultSetReader> fn) {
        interceptor.beforeExecute(QueryType.SCAN_QUERY, delegate, query);
        return delegate.executeScanQuery(query, params, settings, fn);
    }

    @Override
    public CompletableFuture<Result<SessionStatus>> keepAlive(KeepAliveSessionSettings settings) {
        return delegate.keepAlive(settings);
    }

    @Override
    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue rows, BulkUpsertSettings settings) {
        return delegate.executeBulkUpsert(tablePath, rows, settings);
    }

    @Override
    public void invalidateQueryCache() {
        delegate.invalidateQueryCache();
    }

    @Override
    public boolean release() {
        return delegate.release();
    }

    @Override
    public CompletableFuture<Status> close(CloseSessionSettings settings) {
        return delegate.close(settings);
    }

    public enum QueryType {
        DATA_QUERY,
        SCAN_QUERY,
    }
}
