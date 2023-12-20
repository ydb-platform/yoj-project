package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import com.google.protobuf.NullValue;
import com.yandex.ydb.ValueProtos;
import lombok.NonNull;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table.View;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.yql.YqlOrderBy;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@SuppressWarnings({"FieldCanBeLocal", "WeakerAccess"})
public abstract class YqlStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT> implements Statement<PARAMS, RESULT> {

    protected static final Collector<ValueProtos.Value.Builder, ValueProtos.Value.Builder, ValueProtos.Value.Builder> itemsCollector =
            Collector.of(ValueProtos.Value::newBuilder, ValueProtos.Value.Builder::addItems, (b, b2) -> b.addAllItems(b2.getItemsList()));

    protected static final YqlOrderBy ORDER_BY_ID_ASCENDING = YqlOrderBy.orderBy(EntityIdSchema.ID_FIELD_NAME);

    protected final EntitySchema<ENTITY> schema;
    protected final Schema<RESULT> resultSchema;
    protected final ResultSetReader<RESULT> resultSetReader;
    protected final String tableName;

    public YqlStatement(@NonNull EntitySchema<ENTITY> schema, @NonNull Schema<RESULT> resultSchema) {
        this(schema, resultSchema, schema.getName());
    }

    public YqlStatement(@NonNull EntitySchema<ENTITY> schema, @NonNull Schema<RESULT> resultSchema, @NonNull String tableName) {
        this.schema = schema;
        this.resultSchema = resultSchema;
        this.resultSetReader = new ResultSetReader<>(resultSchema);
        this.tableName = tableName;
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> insert(
            Class<ENTITY> type
    ) {
        return new InsertYqlStatement<>(type);
    }

    public static <ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>> Statement<UpdateModel.ById<ID>, ?> update(
            Class<ENTITY> type,
            UpdateModel.ById<ID> model
    ) {
        return new UpdateByIdStatement<>(type, model);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> save(
            Class<ENTITY> type
    ) {
        return new UpsertYqlStatement<>(type);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> find(
            Class<ENTITY> type
    ) {
        EntitySchema<ENTITY> schema = EntitySchema.of(type);
        return find(schema, schema);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, VIEW extends View> Statement<PARAMS, VIEW> find(
            Class<ENTITY> type,
            Class<VIEW> viewType
    ) {
        return find(EntitySchema.of(type), ViewSchema.of(viewType));
    }

    private static <PARAMS, ENTITY extends Entity<ENTITY>, RESULT> Statement<PARAMS, RESULT> find(
            EntitySchema<ENTITY> schema,
            Schema<RESULT> resultSchema) {
        return new YqlStatement<>(schema, resultSchema) {
            @Override
            public List<YqlStatementParam> getParams() {
                return schema.flattenId().stream()
                        .map(c -> YqlStatementParam.required(YqlType.of(c), c.getName()))
                        .collect(toList());
            }

            @Override
            public String getQuery(String tablespace) {
                return declarations()
                        + "SELECT " + outNames()
                        + " FROM " + table(tablespace)
                        + " WHERE " + nameEqVars();
            }

            @Override
            public List<RESULT> readFromCache(PARAMS params, RepositoryCache cache) {
                RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), params);
                if (!cache.contains(key)) {
                    return null;
                }

                //noinspection unchecked
                return cache.get(key)
                        .map(o -> Collections.singletonList((RESULT) o))
                        .orElse(Collections.emptyList());
            }

            @Override
            public void storeToCache(PARAMS params, List<RESULT> result, RepositoryCache cache) {
                RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), params);
                cache.put(key, result.stream().findFirst().orElse(null));
            }

            @Override
            public QueryType getQueryType() {
                return QueryType.SELECT;
            }

            @Override
            public String toDebugString(PARAMS params) {
                return "find(" + params + ")";
            }
        };
    }

    public static <ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ENTITY> findRange(
            Class<ENTITY> type,
            Range<ID> range
    ) {
        EntitySchema<ENTITY> schema = EntitySchema.of(type);
        return new FindRangeStatement<>(schema, schema, range);
    }

    public static <ENTITY extends Entity<ENTITY>, VIEW extends View, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, VIEW> findRange(
            Class<ENTITY> type,
            Class<VIEW> viewType,
            Range<ID> range
    ) {
        return new FindRangeStatement<>(EntitySchema.of(type), ViewSchema.of(viewType), range);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>> Statement<PARAMS, ID> findIdsIn(
            Class<ENTITY> type,
            Iterable<ID> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    ) {
        return new FindInStatement<>(EntitySchema.of(type), EntityIdSchema.ofEntity(type), ids, filter, orderBy, limit);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> findIn(
            Class<ENTITY> type,
            Iterable<? extends Entity.Id<ENTITY>> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    ) {
        return new FindInStatement<>(EntitySchema.of(type), EntitySchema.of(type), ids, filter, orderBy, limit);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, VIEW extends View> Statement<PARAMS, VIEW> findIn(
            Class<ENTITY> type,
            Class<VIEW> viewType,
            Iterable<? extends Entity.Id<ENTITY>> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    ) {
        return new FindInStatement<>(EntitySchema.of(type), ViewSchema.of(viewType), ids, filter, orderBy, limit);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, K> Statement<PARAMS, ENTITY> findIn(
            Class<ENTITY> type,
            String indexName,
            Iterable<K> keys,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    ) {
        return new FindInStatement<>(EntitySchema.of(type), EntitySchema.of(type), indexName, keys, filter, orderBy, limit);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, VIEW extends View, K> Statement<PARAMS, VIEW> findIn(
            Class<ENTITY> type,
            Class<VIEW> viewType,
            String indexName,
            Iterable<K> keys,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    ) {
        return new FindInStatement<>(EntitySchema.of(type), ViewSchema.of(viewType), indexName, keys, filter, orderBy, limit);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> findAll(
            Class<ENTITY> type
    ) {
        EntitySchema<ENTITY> schema = EntitySchema.of(type);
        return findAll(schema, schema);
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>, VIEW extends View> Statement<PARAMS, VIEW> findAll(
            Class<ENTITY> type,
            Class<VIEW> viewType
    ) {
        return findAll(EntitySchema.of(type), ViewSchema.of(viewType));
    }

    private static <PARAMS, ENTITY extends Entity<ENTITY>, RESULT> Statement<PARAMS, RESULT> findAll(
            EntitySchema<ENTITY> schema,
            Schema<RESULT> outSchema
    ) {
        return new FindAllYqlStatement<>(schema, outSchema);
    }

    public static <ENTITY extends Entity<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, ENTITY> find(
            Class<ENTITY> type,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        EntitySchema<ENTITY> schema = EntitySchema.of(type);
        return find(schema, schema, false, parts);
    }

    public static <ENTITY extends Entity<ENTITY>, VIEW extends View> Statement<Collection<? extends YqlStatementPart<?>>, VIEW> find(
            Class<ENTITY> type,
            Class<VIEW> viewType,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        return find(type, viewType, false, parts);
    }

    public static <ENTITY extends Entity<ENTITY>, VIEW extends View> Statement<Collection<? extends YqlStatementPart<?>>, VIEW> find(
            Class<ENTITY> type,
            Class<VIEW> viewType,
            boolean distinct,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        return find(EntitySchema.of(type), ViewSchema.of(viewType), distinct, parts);
    }

    public static <ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, ID> findIds(
            Class<ENTITY> type,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        return find(EntitySchema.of(type), EntityIdSchema.ofEntity(type), false, parts);
    }

    public static <ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ID> findIds(
            Class<ENTITY> type,
            Range<ID> range
    ) {
        return new FindRangeStatement<>(EntitySchema.of(type), EntityIdSchema.ofEntity(type), range);
    }

    @Override
    public void storeToCache(PARAMS params, List<RESULT> result, RepositoryCache cache) {
        if (result == null) {
            return;
        }
        for (Object o : result) {
            if (o instanceof Entity) {
                Entity<?> e = (Entity<?>) o;
                cache.put(new RepositoryCache.Key(e.getClass(), e.getId()), e);
            } else {
                // list should contains elements of the same type
                break;
            }
        }
    }

    public String getDeclaration(String name, String type) {
        return String.format("DECLARE %s AS %s;\n", name, type);
    }

    private static <ENTITY extends Entity<ENTITY>, RESULT> Statement<Collection<? extends YqlStatementPart<?>>, RESULT> find(
            EntitySchema<ENTITY> schema,
            Schema<RESULT> resultSchema,
            boolean distinct,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        List<YqlStatementPart<?>> partList = new ArrayList<>(parts);
        if (!distinct) {
            if (parts.stream().noneMatch(s -> s.getType().equals(YqlOrderBy.TYPE))) {
                partList.add(ORDER_BY_ID_ASCENDING);
            }
        }
        return new PredicateStatement<>(schema, resultSchema, parts, YqlStatement::predicateFrom) {
            @Override
            public String getQuery(String tablespace) {
                return declarations()
                        + "SELECT " + (distinct ? "DISTINCT " : "") + outNames()
                        + " FROM " + table(tablespace)
                        + " " + mergeParts(partList.stream())
                        .sorted(comparing(YqlStatementPart::getPriority))
                        .map(sp -> sp.toFullYql(schema))
                        .map(this::resolveParamNames)
                        .collect(joining(" "));
            }

            @Override
            public QueryType getQueryType() {
                return QueryType.SELECT;
            }

            @Override
            public String toDebugString(Collection<? extends YqlStatementPart<?>> yqlStatementParts) {
                return "find(" + yqlStatementParts + ")";
            }
        };
    }

    public static <ENTITY extends Entity<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, Count> count(
            Class<ENTITY> entityType,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        return count(EntitySchema.of(entityType), parts);
    }

    private static <ENTITY extends Entity<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, Count> count(
            EntitySchema<ENTITY> schema,
            Collection<? extends YqlStatementPart<?>> parts
    ) {
        return new PredicateStatement<>(schema, ObjectSchema.of(Count.class), parts, YqlStatement::predicateFrom) {

            @Override
            public String getQuery(String tablespace) {
                return declarations()
                        + "SELECT COUNT(*) AS count"
                        + " FROM " + table(tablespace)
                        + " " + mergeParts(parts.stream())
                        .sorted(comparing(YqlStatementPart::getPriority))
                        .map(sp -> sp.toFullYql(schema))
                        .map(this::resolveParamNames)
                        .collect(joining(" "));
            }

            @Override
            public QueryType getQueryType() {
                return QueryType.SELECT;
            }

            @Override
            public String toDebugString(Collection<? extends YqlStatementPart<?>> yqlStatementParts) {
                return "count(" + parts + ")";
            }
        };
    }

    protected static YqlPredicate predicateFrom(Collection<? extends YqlStatementPart<?>> parts) {
        return parts.stream()
                .filter(p -> p instanceof YqlPredicate)
                .map(YqlPredicate.class::cast)
                .reduce(YqlPredicate.alwaysTrue(), (p1, p2) -> p1.and(p2));
    }

    protected static Stream<? extends YqlStatementPart<?>> mergeParts(Stream<? extends YqlStatementPart<?>> origParts) {
        return origParts
                .collect(groupingBy(YqlStatementPart::getType))
                .values().stream()
                .flatMap(items -> combine(items).stream());
    }

    private static List<? extends YqlStatementPart<?>> combine(List<? extends YqlStatementPart<?>> items) {
        if (items.size() < 2) {
            return items;
        }
        YqlStatementPart<?> first = items.iterator().next();
        //noinspection unchecked, rawtypes
        return (List<? extends YqlStatementPart<?>>) first.combine((List) items.subList(1, items.size()));
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> deleteAll(Class<ENTITY> type) {
        return new YqlStatement.Simple<>(type) {

            @Override
            public String getQuery(String tablespace) {
                return "DELETE FROM " + table(tablespace);
            }

            @Override
            public QueryType getQueryType() {
                return QueryType.DELETE_ALL;
            }

            @Override
            public String toDebugString(PARAMS params) {
                return "deleteAll(" + schema.getName() + ")";
            }
        };
    }

    public static <PARAMS, ENTITY extends Entity<ENTITY>> Statement<PARAMS, ENTITY> delete(Class<ENTITY> type) {
        return new DeleteByIdStatement<>(type);
    }

    @Override
    public boolean isPreparable() {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, ValueProtos.TypedValue> toQueryParameters(PARAMS params) {
        Map<String, Object> values = params.getClass().isAssignableFrom(schema.getType()) ?
                schema.flatten((ENTITY) params) : schema.flattenId((Entity.Id<ENTITY>) params);
        return getParams().stream()
                .filter(p -> values.containsKey(p.getName()))
                .collect(toMap(YqlStatementParam::getVar, p -> createTQueryParameter(p.getType(),
                        values.get(p.getName()), p.isOptional())));
    }

    protected ValueProtos.TypedValue createTQueryParameter(YqlType type, Object o, boolean optional) {
        return ValueProtos.TypedValue.newBuilder().setType(getYqlType(type, optional)).setValue(getYqlValue(type, o)).build();
    }

    protected ValueProtos.Type.Builder getYqlType(YqlType yqlType, boolean optional) {
        ValueProtos.Type.Builder ttype = yqlType.getYqlTypeBuilder();
        return !optional ? ttype : ValueProtos.Type.newBuilder().setOptionalType(ValueProtos.OptionalType.newBuilder().setItem(ttype));

    }

    protected ValueProtos.Value.Builder getYqlValue(YqlType type, Object value) {
        return value == null ? ValueProtos.Value.newBuilder().setNullFlagValue(NullValue.NULL_VALUE) : type.toYql(value);
    }

    @Override
    public RESULT readResult(List<ValueProtos.Column> columns, ValueProtos.Value value) {
        return resultSetReader.readResult(columns, value);
    }

    @Override
    public String toString() {
        return getQuery("");
    }

    public boolean equals(Object o) {
        return o == this || o instanceof YqlStatement && ((YqlStatement<?, ?, ?>) o).getQuery("").equals(getQuery(""));
    }

    public int hashCode() {
        return getQuery("").hashCode();
    }

    public Class<ENTITY> getInSchemaType() {
        return schema.getType();
    }

    public @NonNull String getTableName() {
        return tableName;
    }

    protected Collection<YqlStatementParam> getParams() {
        return emptyList();
    }

    protected String declarations() {
        return getParams().stream()
                .map(p -> getDeclaration(p.getVar(), p.getType().getYqlTypeName() + (p.isOptional() ? "?" : "")))
                .collect(joining());
    }

    protected String outNames() {
        return resultSchema.flattenFields().stream()
                .map(EntitySchema.JavaField::getName)
                .map(this::escape)
                .collect(joining(", "));
    }

    protected String nameEqVars() {
        return getParams().stream()
                .map(p -> escape(p.getName()) + " = " + p.getVar())
                .collect(joining(" AND "));
    }

    protected String table(String tablespace) {
        return escape(tablespace + tableName);
    }

    protected String escape(String value) {
        // TODO: disallow special characters and ` in table paths/column names
        // or use C-style escapes for them
        return "`" + value + "`";
    }

    /**
     * Resolves {@code ?} placeholders to respective statement parameters' names, and
     * {@code {entity.java.field}} placeholders to DB field names.
     *
     * @param yql YQL with parameter and field name placeholders ({@code ?} and {@code {field.name}}, respectively)
     * @return YQL with real parameter names
     */
    protected String resolveParamNames(String yql) {
        StringBuilder newYql = new StringBuilder();

        Spliterator<YqlStatementParam> paramSpliter = getParams().spliterator();
        int paramCount = 0;
        boolean inFieldPlaceholder = false;
        StringBuilder fieldPlaceholder = new StringBuilder();
        for (int i = 0; i < yql.length(); i++) {
            char ch = yql.charAt(i);

            if (inFieldPlaceholder) {
                switch (ch) {
                    case '{':
                        throw new IllegalStateException("Nested field placeholders are prohibited");
                    case '}':
                        String fieldPath = fieldPlaceholder.toString();
                        EntitySchema.JavaField field = schema.getField(fieldPath);
                        Preconditions.checkState(field.isSimple(),
                                "%s: only simple fields can be referenced using {field.subfield} syntax", fieldPath);
                        newYql.append(field.getName());

                        fieldPlaceholder.setLength(0);
                        inFieldPlaceholder = false;

                        break;
                    case '?':
                        throw new IllegalStateException("Parameter substitution inside field placeholders is prohibited");
                    default:
                        fieldPlaceholder.append(ch);
                        break;
                }
            } else {
                switch (ch) {
                    case '{':
                        inFieldPlaceholder = true;
                        break;
                    case '}':
                        throw new IllegalStateException("Dangling closing curly brace } at <yql>:" + (i + 1));
                    case '?':
                        // insert var name for the next parameter
                        paramCount++;
                        if (!paramSpliter.tryAdvance(param -> newYql.append(param.getVar()))) {
                            throw new IllegalStateException(format(
                                    "Parameter list is too small: expected at least %d parameters, but got: %d",
                                    paramCount, paramCount - 1
                            ));
                        }
                        break;
                    default:
                        newYql.append(ch);
                        break;
                }
            }
        }

        return newYql.toString();
    }

    protected static abstract class Simple<PARAMS, ENTITY extends Entity<ENTITY>>
            extends YqlStatement<PARAMS, ENTITY, ENTITY> {
        public Simple(@NonNull Class<ENTITY> type) {
            super(EntitySchema.of(type), EntitySchema.of(type));
        }
    }
}
