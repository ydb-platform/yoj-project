package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.With;
import lombok.experimental.FieldDefaults;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.List;

import static lombok.AccessLevel.PRIVATE;

/**
 * Represents a {@code view [index_name]} clause in a YQL statement, i.e. index usage
 *
 * @see #toYql(EntitySchema)
 */
@EqualsAndHashCode
@FieldDefaults(makeFinal = true, level = PRIVATE)
public class YqlView implements YqlStatementPart<YqlView> {
    public static final String TYPE = "VIEW";
    public static final YqlView EMPTY = new YqlView("");

    @With
    String index;

    private YqlView(String index) {
        Preconditions.checkArgument(index != null, "index cannot be null");
        this.index = index;
    }

    /**
     * Creates a view clause to fetch rows using effective index
     *
     * @param index index
     * @return view clause to fetch rows using index
     */
    public static YqlView index(String index) {
        return new YqlView(index);
    }

    /**
     * @return view clause that uses no index
     * @see #EMPTY
     */
    public static YqlView empty() {
        return EMPTY;
    }

    public String index() {
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

        throw new IllegalStateException(String.format("Unable to find index [%s] in table [%s]",
                index, schema.getName()));
    }

    @Override
    public List<? extends YqlStatementPart<?>> combine(@NonNull List<? extends YqlView> other) {
        throw new UnsupportedOperationException("Multiple VIEW specifications are not supported");
    }

    @Override
    public String toString() {
        return "view [" + index + "]";
    }
}
