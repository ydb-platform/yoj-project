package tech.ydb.yoj.repository.db.internal;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.RepositoryTransaction;
import tech.ydb.yoj.repository.db.TxManager;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;

import javax.annotation.Nullable;

import static lombok.AccessLevel.PRIVATE;

/**
 * Describes a single loggable database action performed by a {@link RepositoryTransaction}, typically in a transaction.
 * <p>For ease of use, actions that don't take any arguments are available as constants:
 * <ul>
 * <li>{@link InitialDbAction#GET_SESSION}</li>
 * <li>{@link FinalDbAction#COMMIT}</li>
 * <li>{@link FinalDbAction#ROLLBACK}</li>
 * </ul>
 */
@InternalApi
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public sealed interface LoggableDbAction permits
        LoggableDbAction.InitialDbAction,
        LoggableDbAction.QueryDbAction,
        LoggableDbAction.FinalDbAction {
    /**
     * @return human-readable string describing what the database action does
     */
    @NonNull
    String toDebugString();

    /**
     * Database actions that must occur before the transaction starts or just after the transaction has started.
     *
     * @see #GET_SESSION
     */
    sealed interface InitialDbAction extends LoggableDbAction permits InitialDbAction.GetSession {
        /**
         * Request to obtain a database session (connection), most likely from a pool.
         * <p>Not all {@link RepositoryTransaction} implementations use this request type.
         */
        GetSession GET_SESSION = new GetSession();

        /**
         * Request to obtain a database session (connection), most likely from a pool.
         * <p>Not all {@link RepositoryTransaction} implementations use this request type.
         */
        @NoArgsConstructor(access = PRIVATE)
        final class GetSession implements InitialDbAction {
            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "getSession()";
            }
        }
    }

    /**
     * Database actions that must be the last in transaction: either a {@link #COMMIT commit}
     * or a {@link #ROLLBACK rollback}.
     *
     * @see #COMMIT
     * @see #ROLLBACK
     */
    sealed interface FinalDbAction extends LoggableDbAction permits FinalDbAction.Commit, FinalDbAction.Rollback {
        /**
         * Request to commit the transaction.
         */
        Commit COMMIT = new Commit();

        /**
         * Request to rollback the transaction.
         */
        Rollback ROLLBACK = new Rollback();

        /**
         * Request to commit the transaction.
         */
        @NoArgsConstructor(access = PRIVATE)
        final class Commit implements FinalDbAction {
            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "commit()";
            }
        }

        /**
         * Request to rollback the transaction.
         */
        @NoArgsConstructor(access = PRIVATE)
        final class Rollback implements FinalDbAction {
            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "rollback()";
            }
        }
    }

    /**
     * Database actions that perform queries; these are intermediate actions (not initial and not final in the YOJ
     * <em>business transaction</em>).
     */
    sealed interface QueryDbAction extends LoggableDbAction permits
            QueryDbAction.DataQuery, QueryDbAction.ScanQuery,
            QueryDbAction.ReadTable,
            QueryDbAction.BulkUpsert {
        /**
         * Execute a single statement in ordinary ("data query") mode.
         * <p>Consistency and allowed operations (reads and writes, or only reads) depend on the current DB transaction
         * {@link IsolationLevel isolation level}.
         */
        @RequiredArgsConstructor
        final class DataQuery implements QueryDbAction {
            @Nullable
            private final Object log;

            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return String.valueOf(log);
            }
        }

        /**
         * Execute a single statement in scan query mode. Not quite a transactional action, but users may consider
         * scan query isolation to be that of the {@link IsolationLevel#SNAPSHOT_READ_ONLY} isolation level.
         */
        @RequiredArgsConstructor
        final class ScanQuery implements QueryDbAction {
            @Nullable
            private final Object log;

            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "scan()." + log;
            }
        }

        /**
         * Stream the whole table or a range of it. This is not a transactional action, though YOJ API requires it to
         * be run from a <em>business transaction</em> established by {@link TxManager#readOnly()}.
         */
        @RequiredArgsConstructor
        final class ReadTable implements QueryDbAction {
            @NonNull
            private final String tableName;

            @NonNull
            private final ReadTableParams<?> params;

            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "readTable(" + tableName + ", " + params + ")";
            }
        }

        /**
         * Bulk upsert data (non-transactional). This is not a DB-transactional action, though YOJ API requires it to
         * be run from a <em>business transaction</em> established by {@link TxManager#tx(Runnable)}.
         * Internally, a YDB bulk upsert is typically performed in multiple different write transactions.
         */
        @RequiredArgsConstructor
        final class BulkUpsert implements QueryDbAction {
            @NonNull
            private final String tableName;

            private final long size;

            @NonNull
            private final BulkParams params;

            @Override
            public String toString() {
                return toDebugString();
            }

            @NonNull
            @Override
            public String toDebugString() {
                return "bulkUpsert(" + tableName + ", [" + size + "], " + params + ")";
            }
        }
    }
}
