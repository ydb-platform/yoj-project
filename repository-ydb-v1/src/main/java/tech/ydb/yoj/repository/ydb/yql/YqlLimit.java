package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.With;
import lombok.experimental.FieldDefaults;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.List;

import static lombok.AccessLevel.PRIVATE;

/**
 * Represents a {@code LIMIT ... [OFFSET ...]} clause in a YQL statement.
 *
 * @see #top(long)
 * @see #range(long, long)
 * @see #toYql(EntitySchema)
 */
@EqualsAndHashCode
@FieldDefaults(makeFinal = true, level = PRIVATE)
public final class YqlLimit implements YqlStatementPart<YqlLimit> {
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
     * @param to   last row index, counting from 0, exclusive
     * @return limit clause to fetch rows in range {@code [from, to)}
     */
    public static YqlLimit range(long from, long to) {
        Preconditions.checkArgument(from >= 0, "from must be >= 0");
        Preconditions.checkArgument(to >= 0, "to must be >= 0");
        Preconditions.checkArgument(to >= from, "to must be >= from");

        long limit = to - from;
        long offset = limit == 0 ? 0 : from;
        return limit == 0 ? EMPTY : new YqlLimit(limit, offset);
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

    public long size() {
        return limit;
    }

    public boolean isEmpty() {
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
        return size() + (offset == 0 ? "" : " OFFSET " + offset);
    }

    @Override
    public List<? extends YqlStatementPart<?>> combine(@NonNull List<? extends YqlLimit> other) {
        throw new UnsupportedOperationException("Multiple LIMIT specifications are not supported");
    }

    @Override
    public String toString() {
        return "limit " + limit + (offset == 0 ? "" : " offset " + offset);
    }
}
