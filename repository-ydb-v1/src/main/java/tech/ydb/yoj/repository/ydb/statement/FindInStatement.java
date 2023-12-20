package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.yandex.ydb.ValueProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.expression.OrderExpression.SortKey;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlListingQuery;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * <p>Creates statement for {@code SELECT ... WHERE PK IN (PK1, PK2, ...)}. {@code PK} can be both
 * a primary key and a key of secondary indexes. In the case of a secondary index, you must specify
 * the name of the index used.</p>
 *
 * <p>For the entity:</p>
 * <pre>
 *  &#64;GlobalIndex(name = "index_by_keys", fields = {"k1", "k2"})
 *  public class Sample implements Entity&lt;Sample&gt; {
 *    Id id;
 *    String value;
 *    int k1;
 *    String k2;
 *
 *    public static class Id implements Entity.Id&lt;Sample&gt; {
 *      int id1;
 *      int id2;
 *    }
 *  }
 * </pre>
 * <p>The statement will be :</p>
 * <pre>
 * DECLARE $Input AS List&lt;Struct&lt;id1:Int32?,id2:Int32?&gt;&gt;;
 * SELECT t.`id1` AS `id1`, t.`id2` AS `id2`, t.`value` AS `value`
 * FROM AS_TABLE($Input) AS k
 * JOIN `sample` AS t
 * ON t.`id1` = k.`id1` AND t.`id2` = k.`id2`
 * ORDER BY id1 ASC, id2 ASC
 * </pre>
 * <p>If the entity ID is complex type, a part of the ID fields can be null.
 * They will be omitted in the query. For the given example,
 * if <b>all ids</b> have {@code id2 == null}, the statement will be:</p>
 * <pre>
 * DECLARE $Input AS List&lt;Struct&lt;id1:Int32?&gt;&gt;;
 * SELECT t.`id1` AS `id1`, t.`id2` AS `id2`, t.`value` AS `value`
 * FROM AS_TABLE($Input) AS k
 * JOIN `sample` AS t
 * ON t.`id1` = k.`id1`
 * ORDER BY id1 ASC, id2 ASC
 * </pre>
 * <p>If {@code orderBy} specified as order by {@code value} descending, than statement will be:</p>
 * <pre>
 * DECLARE $Input AS List&lt;Struct&lt;id1:Int32?,id2:Int32?&gt;&gt;;
 * SELECT t.`id1` AS `id1`, t.`id2` AS `id2`, t.`value` AS `value`
 * FROM AS_TABLE($Input) AS k
 * JOIN `sample` AS t
 * ON t.`id1` = k.`id1` AND t.`id2` = k.`id2`
 * ORDER BY value DESC
 * </pre>
 * <p>YDB restriction of the ORDER BY clause:
 * ordering fields must be present in the resultSchema.</p>
 * <p>Statement also support {@code filter} expression. With the {@code filter} statement will will be as follows:</p>
 * <pre>
 * DECLARE $Input AS List&lt;Struct&lt;id1:Int32?,id2:Int32?&gt;&gt;;
 * DECLARE $pred_0_value AS 'Utf8?';
 * SELECT `id1`, `id2`, `value`
 * FROM (
 * SELECT t.`id1` AS `id1`, t.`id2` AS `id2`, t.`value` AS `value`
 * FROM AS_TABLE($Input) AS k
 * JOIN `sample` AS t
 * ON t.`id1` = k.`id1` AND t.`id2` = k.`id2`
 * )
 * WHERE value = $pred_0_value
 * ORDER BY id1 ASC, id2 ASC
 * </pre>
 * <p>When using a secondary index, the statement will look like this:</p>
 * <pre>
 * DECLARE $Input AS List&lt;Struct&lt;`k1`:Int32,`k2`:Utf8&gt;&gt;;
 * SELECT t.`value` AS `value`
 * FROM AS_TABLE($Input) AS k
 * JOIN `sample` VIEW `index_by_keys` AS t
 * ON t.`k1` = k.`k1` AND t.`k2` = k.`k2`
 * </pre>
 */
public final class FindInStatement<IN, T extends Entity<T>, RESULT> extends MultipleVarsYqlStatement<IN, T, RESULT> {
    private static final Logger log = LoggerFactory.getLogger(FindInStatement.class);

    private final String indexName;
    private final Schema<?> keySchema;
    private final Set<String> keyFields;
    private final PredicateClause<T> predicate;
    private final OrderExpression<T> orderBy;
    private final Integer limit;

    /**
     * Creates new {@code FindInStatement} instance with pagination.
     *
     * @param schema       entity schema
     * @param resultSchema result schema
     * @param ids          the ids of entities to load
     * @param filter       optional additional filter expression, the filter is expected to filter by non-PK fields
     * @param orderBy      order by expression for result sorting, order fields must be present
     *                     in the {@code resultSchema}
     */
    protected FindInStatement(
            EntitySchema<T> schema,
            Schema<RESULT> resultSchema,
            Iterable<? extends Entity.Id<T>> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        super(schema, resultSchema);

        this.orderBy = orderBy;
        this.limit = limit;

        indexName = null;
        keySchema = schema.getIdSchema();
        keyFields = collectKeyFieldsFromIds(schema.getIdSchema(), ids);
        predicate = (filter == null) ? null : new PredicateClause<>(schema, YqlListingQuery.toYqlPredicate(filter));

        validateOrderByFields();
    }

    /**
     * Creates new {@code FindInStatement} instance with index usage and pagination.
     */
    protected <V> FindInStatement(
            EntitySchema<T> schema,
            Schema<RESULT> resultSchema,
            String indexName,
            Iterable<V> keys,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        super(schema, resultSchema);

        this.indexName = indexName;
        this.orderBy = orderBy;
        this.limit = limit;

        Schema<V> schemaFromValues = getKeySchemaFromValues(keys);

        keySchema = schemaFromValues;
        keyFields = collectKeyFieldsFromKeys(schema, indexName, schemaFromValues, keys);
        predicate = (filter == null) ? null : new PredicateClause<>(schema, YqlListingQuery.toYqlPredicate(filter));

        validateOrderByFields();
    }

    private static <T extends Entity<T>> Set<String> collectKeyFieldsFromIds(
            Schema<Entity.Id<T>> idSchema,
            Iterable<? extends Entity.Id<T>> ids) {
        Preconditions.checkArgument(!Iterables.isEmpty(ids), "ids should be non empty");

        Set<Set<String>> nonNullFieldsSet = Streams.stream(ids)
                .map(id -> nonNullKeyFieldNames(idSchema, id))
                .collect(toUnmodifiableSet());

        Preconditions.checkArgument(nonNullFieldsSet.size() != 0, "ids should have at least one non-null field");
        Preconditions.checkArgument(nonNullFieldsSet.size() == 1, "ids should have nulls in the same fields");

        Set<String> keyFields = Iterables.getOnlyElement(nonNullFieldsSet);
        if (!isPrefixedFields(idSchema.flattenFieldNames(), keyFields)) {
            log.warn("FindIn(ids) not by the primary key prefix will result in a FullScan, PK: {}, query uses the fields: {}",
                    idSchema.flattenFieldNames(), keyFields);
        }

        return keyFields;
    }

    private static <V> Schema<V> getKeySchemaFromValues(Iterable<V> keys) {
        V key = Iterables.getFirst(keys, null);
        Preconditions.checkArgument(key != null, "keys should be non empty");

        return ObjectSchema.of((Class<V>) key.getClass());
    }

    private static <V> Set<String> collectKeyFieldsFromKeys(
            Schema<?> entitySchema,
            String indexName,
            Schema<V> keySchema,
            Iterable<V> keys) {
        Set<Set<String>> nonNullFieldsSet = Streams.stream(keys)
                .map(key -> nonNullKeyFieldNames(keySchema, key))
                .collect(toUnmodifiableSet());

        Preconditions.checkArgument(nonNullFieldsSet.size() != 0, "keys should have at least one non-null field");
        Preconditions.checkArgument(nonNullFieldsSet.size() == 1, "keys should have nulls in the same fields");

        Set<String> keyFields = Iterables.getOnlyElement(nonNullFieldsSet);

        // 1. there is a specified index
        Schema.Index globalIndex = entitySchema.getGlobalIndexes().stream()
                .filter(index -> indexName.equals(index.getIndexName()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Entity `%s` doesn't have index `%s`".formatted(entitySchema.getName(), indexName)
                ));

        // 2. all key fields are index key fields
        Set<String> indexKeys = Set.copyOf(globalIndex.getFieldNames());
        Set<String> missingInIndexKeys = Sets.difference(keyFields, indexKeys);

        Preconditions.checkArgument(
                missingInIndexKeys.isEmpty(),
                "Index `%s` of entity `%s` doesn't contain key(s): [%s]".formatted(
                        indexName, entitySchema.getName(), String.join(", ", missingInIndexKeys)
                )
        );

        // 3. key fields are exactly the same as index key fields or are its prefix
        Preconditions.checkArgument(
                isPrefixedFields(globalIndex.getFieldNames(), keyFields),
                "FindIn(keys) is allowed only by the prefix of the index key fields, index key: %s, query uses the fields: %s"
                        .formatted(globalIndex.getFieldNames(), keyFields)
        );

        // 4. the types of key fields and the types of the corresponding entity fields are the same
        Map<String, Type> keyFieldTypes = getKeyFieldTypeMap(keySchema, keyFields);
        Map<String, Type> entityFieldTypes = getKeyFieldTypeMap(entitySchema, keyFields);

        for (var keyFieldType : keyFieldTypes.entrySet()) {
            var entityFieldType = entityFieldTypes.get(keyFieldType.getKey());

            Preconditions.checkArgument(
                    entityFieldType.equals(keyFieldType.getValue()),
                    "Entity `%s` has column `%s` of type `%s`, but corresponding key field is `%s`".formatted(
                            entitySchema.getName(), keyFieldType.getKey(), entityFieldType, keyFieldType.getValue()
                    )
            );
        }

        return globalIndex.getFieldNames().stream()
                .limit(keyFields.size())
                .collect(toCollection(LinkedHashSet::new));
    }

    private static <V> Set<String> nonNullKeyFieldNames(Schema<V> schema, V key) {
        return schema.flatten(key).keySet();
    }

    private static boolean isPrefixedFields(List<String> keyFields, Set<String> fields) {
        for (var keyField : keyFields.subList(0, fields.size())) {
            if (!fields.contains(keyField)) {
                return false;
            }
        }

        return true;
    }

    private static Map<String, Type> getKeyFieldTypeMap(Schema<?> schema, Set<String> keyFields) {
        return schema.flattenFields().stream()
                .filter(f -> keyFields.contains(f.getName()))
                .collect(toUnmodifiableMap(JavaField::getName, JavaField::getType));
    }

    private void validateOrderByFields() {
        if (!hasOrderBy() || schema.equals(resultSchema)) {
            return;
        }

        Set<String> resultColumns = resultSchema.flattenFields().stream()
                .map(JavaField::getName).collect(toUnmodifiableSet());
        List<String> missingColumns = orderBy.getKeys().stream()
                .map(SortKey::getField)
                .flatMap(JavaField::flatten)
                .map(JavaField::getName)
                .filter(column -> !resultColumns.contains(column))
                .toList();

        Preconditions.checkArgument(
                missingColumns.isEmpty(),
                "Result schema of '%s' does not contain field(s): [%s] by which the result is ordered: %s".formatted(
                        resultSchema.getType().getSimpleName(), String.join(", ", missingColumns), orderBy
                )
        );
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT;
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations() +
                "SELECT " + outNames() + "\n" +
                (hasPredicate() ? "FROM (\nSELECT " + allColumnNames() + "\n" : "") +
                "FROM AS_TABLE(" + listName + ") AS k\n" +
                "JOIN " + table(tablespace) + indexUsage() + " AS t\n" +
                "ON " + joinExpression() + "\n" +
                (hasPredicate() ? ")\n" : "") +
                predicateClause() +
                orderByClause() +
                limitClause();
    }

    @Override
    public List<YqlStatementParam> getParams() {
        return schema.flattenFields().stream()
                .filter(c -> keyFields.contains(c.getName()))
                .map(c -> YqlStatementParam.required(YqlType.of(c), c.getName()))
                .toList();
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(IN in) {
        if (hasPredicate()) {
            return ImmutableMap.<String, ValueProtos.TypedValue>builder()
                    .putAll(super.toQueryParameters(in))
                    .putAll(predicate.toQueryParameters())
                    .build();
        }

        return super.toQueryParameters(in);
    }

    @Override
    protected String declarations() {
        return super.declarations() + predicateClauseDeclarations();
    }

    @Override
    protected String outNames() {
        return resultSchema.flattenFields().stream()
                .map(this::getOutName)
                .collect(joining(", "));
    }

    private String allColumnNames() {
        return schema.flattenFields().stream()
                .map(this::getAliasedName)
                .collect(joining(", "));
    }

    private String getOutName(JavaField field) {
        return hasPredicate() ? escape(field.getName()) : getAliasedName(field);
    }

    private String getAliasedName(JavaField field) {
        String escapedName = escape(field.getName());

        return "t." + escapedName + " AS " + escapedName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Function<IN, Map<String, Object>> flattenInputVariables() {
        return ((Schema<IN>) keySchema)::flatten;
    }

    private String indexUsage() {
        return isFindByIndex() ? " VIEW " + escape(indexName) : "";
    }

    private String joinExpression() {
        return keyFields.stream()
                .map(n -> "t.%1$s = k.%1$s".formatted(escape(n)))
                .collect(joining(" AND "));
    }

    private String orderByClause() {
        return hasOrderBy() ? YqlListingQuery.toYqlOrderBy(orderBy).toFullYql(schema) + "\n" : "";
    }

    private String limitClause() {
        return hasLimit() ? "LIMIT " + limit + "\n" : "";
    }

    private String predicateClauseDeclarations() {
        return hasPredicate() ? predicate.declarations() : "";
    }

    private String predicateClause() {
        return hasPredicate() ? predicate.getClause() : "";
    }

    @Override
    public String toDebugString(IN in) {
        return "findIn(" + toDebugParams(in) +
                (isFindByIndex() ? " by index " + escape(indexName) : "") +
                (hasPredicate() ? ", filter [" + predicate.toDebugString() + "]" : "") +
                (hasOrderBy() ? ", orderBy [" + orderBy + "]" : "") +
                (hasLimit() ? ", limit [" + limit + "]" : "") +
                ")";
    }

    private boolean isFindByIndex() {
        return indexName != null;
    }

    private boolean hasLimit() {
        return limit != null;
    }

    private boolean hasOrderBy() {
        return orderBy != null;
    }

    private boolean hasPredicate() {
        return predicate != null;
    }

    private static class PredicateClause<T extends Entity<T>> extends PredicateStatement<Class<Void>, T, T> {
        private final YqlPredicate predicate;

        public PredicateClause(EntitySchema<T> schema, YqlPredicate predicate) {
            super(schema, schema, Void.class, __ -> predicate);

            this.predicate = predicate;
        }

        @Override
        public QueryType getQueryType() {
            return QueryType.UNTYPED;
        }

        public String getClause() {
            return resolveParamNames(predicate.toFullYql(schema)) + "\n";
        }

        @Override
        public String getQuery(String tablespace) {
            return "SELECT 1";
        }

        public String toDebugString() {
            return toDebugString(Void.TYPE);
        }

        public Map<String, ValueProtos.TypedValue> toQueryParameters() {
            return toQueryParameters(Void.TYPE);
        }

        @Override
        public String toDebugString(Class<Void> in) {
            return predicate.toString();
        }
    }
}
