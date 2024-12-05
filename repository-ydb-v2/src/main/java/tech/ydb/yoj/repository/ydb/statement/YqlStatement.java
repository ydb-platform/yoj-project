package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import com.google.protobuf.NullValue;
import lombok.NonNull;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.yql.YqlOrderBy;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
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

    public YqlStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, Schema<RESULT> resultSchema
    ) {
        this.schema = schema;
        this.resultSchema = resultSchema;
        this.resultSetReader = new ResultSetReader<>(resultSchema);
        this.tableName = tableDescriptor.tableName();
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
     *
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
}
