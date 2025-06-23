package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

/**
 * Represents a {@code LIMIT ... [OFFSET ...]} clause in a YQL statement.
 * <br>Note that YDB does <strong>not</strong> support {@code OFFSET} without {@code LIMIT}, so "row offset" cannot be set directly.
 * To return the maximum possible amount of rows, you must know the YDB ResultSet row limit (e.g., defined by {@code YDB_KQP_RESULT_ROWS_LIMIT}
 * environment variable for local YDB-in-Docker), and use {@link YqlLimit#range(long, long) YqlLimit.range(offset, max rows + offset)}.
 * If you have a specific limit {@code < max rows}, it's much better to use {@link YqlLimit#range(long, long) YqlLimit.range(offset, limit + offset)},
 * of course.
 *
 * @see #top(long)
 * @see #range(long, long)
 * @see #toYql(EntitySchema)
 */
@Value
public class YqlLimit implements YqlStatementPart<YqlLimit> {
    /**
     * Gives a {@code LIMIT} clause that will always return an empty range ({@code LIMIT 0}).
     */
    public static final YqlLimit EMPTY = new YqlLimit(0, 0);

    @With
    long limit;
    @With
    long offset;

    private YqlLimit(long limit, long offset) {
        Preconditions.checkArgument(limit >= 0, "limit must be >= 0");
        Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Creates a limit clause to fetch rows in the half-open range {@code [from, to)}.
     *
     * @param from first row index, counting from 0, inclusive
     * @param to   last row index, counting from 0, exclusive. Must be {@code >= from}
     * @return limit clause to fetch {@code (to - from)} rows in range {@code [from, to)}
     */
    public static YqlLimit range(long from, long to) {
        Preconditions.checkArgument(from >= 0, "from must be >= 0");
        Preconditions.checkArgument(to >= 0, "to must be >= 0");
        Preconditions.checkArgument(to >= from, "to must be >= from");

        return to == from ? EMPTY : new YqlLimit(to - from, from);
    }

    /**
     * Creates a limit clause to fetch top {@code n} rows, as if by calling {@link #range(long, long)
     * range(0, n)}.
     *
     * @param n number of rows to fetch
     * @return limit clause to fetch top {@code n} rows
     */
    public static YqlLimit top(long n) {
        Preconditions.checkArgument(n >= 0, "n must be >= 0");
        return n == 0 ? EMPTY : new YqlLimit(n, 0);
    }

    /**
     * @return limit clause that fetches no rows
     * @see #EMPTY
     */
    public static YqlLimit empty() {
        return EMPTY;
    }

    /**
     * @deprecated Please calculate the maximum number of rows fetched by this {@code LIMIT} clause by using {@code YqlLimit.getLimit()} and
     * {@code YqlLimit.getOffset()} instead.
     */
    @Deprecated(forRemoval = true)
    public long size() {
        DeprecationWarnings.warnOnce("YqlLimit.size()",
                "Please calculate range size using YqlLimit.getLimit() and YqlLimit.getOffset() instead of calling YqlLimit.size()");
        return limit;
    }

    /**
     * @return {@code true} if this {@code YqlLimit} represents an empty range ({@code LIMIT 0}); {@code false} otherwise
     */
    public boolean isEmpty() {
        // Does not need to check the offset because with limit == 0, the offset is irrelevant, the DB will return no rows anyway!
        return limit == 0;
    }

    @Override
    public int getPriority() {
        return 150;
    }

    @Override
    public String getType() {
        return "Limit";
    }

    @Override
    public String getYqlPrefix() {
        return "LIMIT ";
    }

    @Override
    public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
        return limit + (offset == 0 ? "" : " OFFSET " + offset);
    }

    @Override
    public String toString() {
        return "limit " + limit + (offset == 0 ? "" : " offset " + offset);
    }
}
