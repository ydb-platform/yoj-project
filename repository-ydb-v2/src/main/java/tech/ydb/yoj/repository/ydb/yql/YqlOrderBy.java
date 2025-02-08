package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;
import static tech.ydb.yoj.repository.ydb.yql.YqlOrderBy.SortOrder.ASC;
import static tech.ydb.yoj.repository.ydb.yql.YqlOrderBy.SortOrder.DESC;

@Getter
@EqualsAndHashCode
public final class YqlOrderBy implements YqlStatementPart<YqlOrderBy> {
    public static final String TYPE = "OrderBy";
    private final List<SortKey> keys;

    private YqlOrderBy(@NonNull List<SortKey> keys) {
        validateKeys(keys);
        this.keys = ImmutableList.copyOf(keys);
    }

    private static void validateKeys(@NonNull List<SortKey> keys) {
        Set<String> uniqueFieldPaths = new HashSet<>();
        for (SortKey key : keys) {
            String fieldPath = key.fieldPath;
            if (!uniqueFieldPaths.add(fieldPath)) {
                throw new IllegalArgumentException(format(
                        "Field \"%s\" is mentioned multiple times in ORDER BY",
                        fieldPath
                ));
            }
        }
    }

    /**
     * Orders entities by the specified fields in <em>ascending order</em>.
     *
     * @param fieldPath       first field's path
     * @param otherFieldPaths the rest of the fields' paths
     * @return {@code ORDER BY <field1> ASC, ..., <fieldN> ASC}
     * @see #order()
     */
    public static YqlOrderBy orderBy(String fieldPath, String... otherFieldPaths) {
        return new Builder().asc(fieldPath, otherFieldPaths).build();
    }

    /**
     * Orders entities by the specified field in the specified order.
     *
     * @param fieldPath first field's path
     * @param order     sort order
     * @return {@code ORDER BY <field> <sort order>}
     * @see #order()
     */
    public static YqlOrderBy orderBy(String fieldPath, SortOrder order) {
        return new Builder().by(fieldPath, order).build();
    }

    /**
     * Orders entities by the specified {@link SortKey keys}.
     *
     * @param key       first sort key
     * @param otherKeys remaining sort keys
     * @return {@code ORDER BY <field 1> <sort order 1>, ..., <field N> <sort order N>}
     * @see SortKey
     * @see #order()
     */
    public static YqlOrderBy orderBy(SortKey key, SortKey... otherKeys) {
        return new Builder().by(key, otherKeys).build();
    }

    /**
     * Orders entities by the specified {@link SortKey keys}.
     *
     * @param keys sort keys
     * @return {@code ORDER BY <field 1> <sort order 1>, ..., <field N> <sort order N>}
     * @see SortKey
     * @see #order()
     */
    public static YqlOrderBy orderBy(Collection<SortKey> keys) {
        return new Builder().by(keys).build();
    }

    /**
     * @return builder for {@code YqlOrderBy}
     */
    public static Builder order() {
        return new Builder();
    }

    @Override
    public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
        return keys.stream().map(k -> k.toYql(schema)).collect(joining(", "));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getYqlPrefix() {
        return "ORDER BY ";
    }

    @Override
    public int getPriority() {
        return 100;
    }

    @Override
    public List<? extends YqlOrderBy> combine(@NonNull List<? extends YqlOrderBy> other) {
        return singletonList(new YqlOrderBy(other.stream()
                .flatMap(o -> o.getKeys().stream())
                .collect(toList())));
    }

    @Override
    public String toString() {
        return format("order by %s", keys.stream().map(Object::toString).collect(joining(", ")));
    }

    public enum SortOrder {
        ASC("ASC"),
        DESC("DESC");

        private final String yql;

        SortOrder(String yql) {
            this.yql = yql;
        }

        @Override
        public String toString() {
            return yql;
        }
    }

    /**
     * Sort key: entity field plus {@link SortOrder sort order} (either ascending or descending).
     */
    @Value
    public static final class SortKey {
        String fieldPath;
        SortOrder order;

        private <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            return schema.getField(fieldPath).flatten()
                    .map(jf -> format("`%s` %s", jf.getName(), order))
                    .collect(joining(", "));
        }

        @Override
        public String toString() {
            return format("%s %s", fieldPath, order);
        }
    }

    public static final class Builder {
        private final ImmutableList.Builder<SortKey> keyBldr = ImmutableList.builder();

        private Builder() {
        }

        public Builder asc(String fieldPath, String... otherFieldPaths) {
            concat(Stream.of(fieldPath), stream(otherFieldPaths))
                    .map(fp -> new SortKey(fp, ASC))
                    .forEach(keyBldr::add);
            return this;
        }

        public Builder desc(String fieldPath, String... otherFieldPaths) {
            concat(Stream.of(fieldPath), stream(otherFieldPaths))
                    .map(fp -> new SortKey(fp, DESC))
                    .forEach(keyBldr::add);
            return this;
        }

        public Builder by(String fieldPath, SortOrder order) {
            return by(new SortKey(fieldPath, order));
        }

        public Builder by(SortKey key, SortKey... otherKeys) {
            this.keyBldr.add(key);
            this.keyBldr.add(otherKeys);
            return this;
        }

        public Builder by(Collection<SortKey> keys) {
            this.keyBldr.addAll(keys);
            return this;
        }

        public YqlOrderBy build() {
            List<SortKey> keys = keyBldr.build();
            Preconditions.checkState(!keys.isEmpty(), "ORDER BY must have at least one key to sort by");

            return new YqlOrderBy(keys);
        }
    }
}
