package tech.ydb.yoj.repository.ydb.yql;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.client.YdbPaths;

import static lombok.AccessLevel.NONE;
import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.validateObjectName;

/**
 * Represents a {@code VIEW ...} clause in a YQL statement, i.e., secondary index usage or explicit usage of primary key
 *
 * @see #toYql(EntitySchema)
 */
@Value
@RequiredArgsConstructor(access = PRIVATE)
public class YqlView implements YqlStatementPart<YqlView> {
    public static final String TYPE = "VIEW";

    /**
     * @deprecated This public static field behaves the same as {@link YqlView#empty()} method.
     * It will be removed in YOJ 3.0.0.
     */
    @Deprecated(forRemoval = true)
    public static final YqlView EMPTY = new YqlView(Type.EMPTY, "");

    private static final YqlView PRIMARY_KEY = new YqlView(Type.PRIMARY_KEY, "");

    @Getter(NONE)
    Type type;

    @With
    @NonNull
    String index;

    /**
     * Creates a view clause to fetch rows using an <em><strong>explicit</strong> secondary index</em>.
     *
     * @param index secondary index name; must not be {@code null} or empty
     * @return view clause to fetch rows using index
     * @see #primaryKey() force fetch by primary key
     * @see #empty() let YDB decide on optimal index(es)
     */
    public static YqlView index(@NonNull String index) {
        YdbPaths.validateObjectName(index);
        return new YqlView(Type.SECONDARY_INDEX, index);
    }

    /**
     * @return view clause that specifies <em><strong>no</strong> explicit index</em>.<br>
     * <em>Note:</em> Modern versions of YDB (25+) <em>might</em> infer an index automatically.
     * @see #primaryKey() force fetch by primary key
     * @see #index(String) fetch by secondary index
     */
    public static YqlView empty() {
        return EMPTY;
    }

    /**
     * @return view clause that specifies <em><strong>explicit</strong> fetch by primary key</em>
     * @see #empty() let YDB decide on optimal index(es)
     * @see #index(String) fetch by secondary index
     */
    public static YqlView primaryKey() {
        return PRIMARY_KEY;
    }

    /**
     * @deprecated This method is confusingly named, because there also is a static constructor {@link YqlView#index(String)}.
     * It will be removed in YOJ 3.0.0. Please use {@link #getIndex()} instead.
     */
    @Deprecated(forRemoval = true)
    public String index() {
        DeprecationWarnings.warnOnce("YqlView.index()",
                "YqlView.index() getter will be removed in YOJ 3.0.0. Please use YqlView.getIndex() instead");
        return index;
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getYqlPrefix() {
        return "";
    }

    @Override
    public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
        return switch (type) {
            case EMPTY -> "";
            case PRIMARY_KEY -> "VIEW PRIMARY KEY";
            case SECONDARY_INDEX -> "VIEW `" + ensureIndexExists(schema, index) + "`";
        };
    }

    private String ensureIndexExists(@NonNull Schema<?> schema, @NonNull String indexName) {
        var ignore = schema.getGlobalIndex(indexName);
        validateObjectName(indexName);
        return indexName;
    }

    @Override
    public String toString() {
        return switch (type) {
            case EMPTY -> "view <autodetect index>";
            case PRIMARY_KEY -> "view primary key";
            case SECONDARY_INDEX -> "view [" + index + "]";
        };
    }

    private enum Type {
        EMPTY,
        PRIMARY_KEY,
        SECONDARY_INDEX
    }
}
