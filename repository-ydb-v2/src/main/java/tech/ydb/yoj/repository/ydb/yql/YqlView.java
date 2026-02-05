package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import static lombok.AccessLevel.PRIVATE;

/**
 * Represents a {@code view [index_name]} clause in a YQL statement, i.e. index usage
 *
 * @see #toYql(EntitySchema)
 */
@Value
@RequiredArgsConstructor(access = PRIVATE)
public class YqlView implements YqlStatementPart<YqlView> {
    public static final String TYPE = "VIEW";
    public static final YqlView EMPTY = new YqlView("");

    @With
    @NonNull
    String index;

    /**
     * Creates a view clause to fetch rows using effective index
     *
     * @param index index name; must not be {@code null}
     * @return view clause to fetch rows using index
     */
    public static YqlView index(@NonNull String index) {
        return new YqlView(index);
    }

    /**
     * @return view clause that uses no index
     * @see #EMPTY
     */
    public static YqlView empty() {
        return EMPTY;
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
        if (Strings.isNullOrEmpty(index)) {
            return "";
        }
        for (Schema.Index idx : schema.getGlobalIndexes()) {
            if (idx.getIndexName().equals(index)) {
                return "VIEW `" + index + "`";
            }
        }

        throw new IllegalStateException(
                "Unable to find index '%s' for entity <%s>".formatted(index, schema.getTypeName())
        );
    }

    @Override
    public String toString() {
        return "view [" + index + "]";
    }
}
