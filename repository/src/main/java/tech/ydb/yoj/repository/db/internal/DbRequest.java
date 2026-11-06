package tech.ydb.yoj.repository.db.internal;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static java.util.Collections.singletonList;
import static lombok.AccessLevel.PRIVATE;

/**
 * Describes a single request to the database performed by a {@link tech.ydb.yoj.repository.db.RepositoryTransaction},
 * typically in a transaction <em>but not always</em>.
 * <p>For ease of use, requests that don't take any arguments are available as constants:
 * <ul>
 * <li>{@link #COMMIT COMMIT - commit()}</li>
 * <li>{@link #ROLLBACK ROLLBACK - rollback()}</li>
 * <li>{@link #GET_SESSION GET_SESSION - getSession()}</li>
 * </ul>
 */
@Value
@InternalApi
@RequiredArgsConstructor(access = PRIVATE)
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/165")
public class DbRequest {
    @NonNull
    Type type;

    @NonNull
    List</*@Nullable*/ ?> args;

    /**
     * Request to commit the transaction.
     */
    public static final DbRequest COMMIT = new DbRequest(Type.COMMIT);
    /**
     * Request to rollback the transaction.
     */
    public static final DbRequest ROLLBACK = new DbRequest(Type.ROLLBACK);
    /**
     * Request to obtain a database session (connection), most likely from a pool.
     */
    public static final DbRequest GET_SESSION = new DbRequest(Type.GET_SESSION);

    private DbRequest(@NonNull Type type) {
        this(type, List.of());
    }

    public static DbRequest dataQuery(@Nullable String log) {
        return new DbRequest(Type.DATA_QUERY, singletonList(log));
    }

    public static DbRequest scanQuery(@Nullable String log) {
        return new DbRequest(Type.SCAN_QUERY, singletonList(log));
    }

    public static DbRequest readTable(@NonNull String tableName, @NonNull ReadTableParams<?> params) {
        return new DbRequest(Type.READ_TABLE, List.of(tableName, params));
    }

    public static DbRequest bulkUpsert(@NonNull String tableName, long size, @NonNull BulkParams bulkParams) {
        return new DbRequest(Type.BULK_UPSERT, List.of(tableName, "[" + size + "]", bulkParams));
    }

    /**
     * Request type, e.g., {@link Type#COMMIT commit()}.
     */
    @NonNull
    public Type getType() {
        return type;
    }

    /**
     * Request arguments, if any. <strong>Human-readable, not machine-readable</strong>.
     * Arguments are not standardized across {@link tech.ydb.yoj.repository.db.RepositoryTransaction} implementations.
     */
    public @NonNull List</*@Nullable*/ ?> getArgs() {
        return args;
    }

    @NonNull
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(type.formatPrefix());

        Iterator<?> iter = args.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }

        sb.append(type.formatSuffix());

        return sb.toString();
    }

    /**
     * Database request type.
     */
    public enum Type {
        /**
         * Commit the transaction.
         */
        COMMIT,

        /**
         * Rollback the transaction.
         */
        ROLLBACK,

        /**
         * Obtain a database session (connection), most likely from a pool.
         * Not all {@link tech.ydb.yoj.repository.db.RepositoryTransaction} implementations use this request type.
         */
        GET_SESSION,

        /**
         * Execute a single statement in ordinary query mode. Consistency and allowed operations (reads and writes,
         * or only reads) depend on the transaction's isolation level.
         */
        DATA_QUERY {
            @Override
            public String formatPrefix() {
                return "";
            }

            @Override
            public String formatSuffix() {
                return "";
            }
        },
        /**
         * Execute a single statement in scan query mode. Not quite a transactional action, but users may consider
         * scan query isolation to be that of the {@link tech.ydb.yoj.repository.db.IsolationLevel#SNAPSHOT_READ_ONLY}
         * isolation level.
         */
        SCAN_QUERY {
            @Override
            public String formatPrefix() {
                return "scan().";
            }

            @Override
            public String formatSuffix() {
                return "";
            }
        },
        /**
         * Stream the whole table or a range of it (non-transactional).
         */
        READ_TABLE,
        /**
         * Bulk upsert data (non-transactional).
         */
        BULK_UPSERT,
        ;

        public String formatPrefix() {
            return UPPER_UNDERSCORE.to(LOWER_CAMEL, name()) + "(";
        }

        public String formatSuffix() {
            return ")";
        }
    }
}
