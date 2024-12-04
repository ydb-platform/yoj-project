package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.Value;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * <p>Creates statement for {@code UPDATE table SET values=values WHERE PK IN (PK1, PK2, ...)}.</p>
 */
@SuppressWarnings("DuplicatedCode")
public class UpdateInStatement<T extends Entity<T>, RESULT>
        extends YqlStatement<UpdateInStatement.UpdateInStatementInput<T>, T, RESULT> {
    public static final String keysParam = "$input_ids";

    private final Map<JavaField, Object> values;
    private final Set<String> keyFields;

    public UpdateInStatement(
            EntitySchema<T> schema,
            Schema<RESULT> resultSchema,
            UpdateInStatementInput<T> in
    ) {
        super(schema, resultSchema);

        this.keyFields = collectKeyFields(in.ids);
        this.values = new HashMap<>(in.values.size());

        for (var entry : in.values.entrySet()) {
            values.put(schema.getField(entry.getKey()), entry.getValue());
        }
    }

    private Set<String> collectKeyFields(Collection<? extends Entity.Id<T>> ids) {
        Preconditions.checkNotNull(ids, "ids should be non null");
        Preconditions.checkArgument(!Iterables.isEmpty(ids), "ids should be non empty");

        var nonNullFieldsSet = ids.stream()
                .map(this::nonNullFieldNames)
                .collect(toUnmodifiableSet());

        Preconditions.checkArgument(nonNullFieldsSet.size() != 0, "ids should have at least one non-null field");
        Preconditions.checkArgument(nonNullFieldsSet.size() == 1, "ids should have nulls in the same fields");

        return Iterables.getOnlyElement(nonNullFieldsSet);
    }

    @Override
    protected String declarations() {
        var valuesDeclaration = values.keySet().stream()
                .map(e -> getDeclaration("$" + e.getPath(), YqlType.of(e).getYqlTypeName()))
                .collect(joining());

        var keysDeclaration = getKeyParams().stream()
                .map(p -> String.format("%s%s", p.getType().getYqlTypeName(), p.isOptional() ? "?" : ""))
                .collect(joining(",", "DECLARE " + keysParam + " AS List<Tuple<", ">>;\n"));

        return keysDeclaration + valuesDeclaration;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.UPDATE;
    }

    @Override
    public String getQuery(String tablespace) {
        // "(`column`) IN " must be written as "(`column`,) IN "
        var keyParams = this.getKeyParams();
        var keys = keyParams.stream()
                .map(p -> this.escape(p.getName()))
                .collect(Collectors.joining(",", "", keyParams.size() == 1 ? "," : ""));

        var setSection = values.keySet().stream()
                .map(x -> x.getName() + "=$" + x.getPath())
                .collect(Collectors.joining(", "));

        return String.format(
                "%sUPDATE%s\nSET %s\nWHERE (%s) IN %s",
                declarations(), table(tablespace), setSection, keys, keysParam
        );
    }

    @Override
    public List<YqlStatementParam> getParams() {
        var params = getValuesParams();
        params.addAll(getKeyParams());
        return params;
    }

    private List<YqlStatementParam> getValuesParams() {
        return this.values.keySet().stream()
                .map(x -> new YqlStatementParam(YqlType.of(x), x.getPath(), false))
                .collect(Collectors.toList());
    }

    private List<YqlStatementParam> getKeyParams() {
        return schema.flattenId().stream()
                .filter(c -> keyFields.contains(c.getName()))
                .map(c -> YqlStatementParam.required(YqlType.of(c), c.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(UpdateInStatementInput<T> params) {
        var idsParam = getIdsQueryParameters(params);

        var valuesParams = getValuesParams().stream()
                .collect(toMap(YqlStatementParam::getVar, p -> createTQueryParameter(p.getType(),
                        params.values.get(p.getName()), p.isOptional())));

        valuesParams.put(keysParam, idsParam);

        return valuesParams;
    }

    private ValueProtos.TypedValue getIdsQueryParameters(UpdateInStatementInput<T> params) {
        var keyParams = getKeyParams();
        var tupleBuilder = ValueProtos.TupleType.newBuilder();
        keyParams.forEach(param -> tupleBuilder.addElements(getYqlType(param.getType(), param.isOptional())));

        var ids = params.ids.stream()
                .map(schema::flattenId)
                .map(fieldValues ->
                        keyParams.stream()
                                .map(p -> getYqlValue(p.getType(), fieldValues.get(p.getName())))
                                .collect(itemsCollector)
                )
                .collect(itemsCollector);

        var tupleListType = ValueProtos.ListType.newBuilder()
                .setItem(ValueProtos.Type.newBuilder().setTupleType(tupleBuilder.build()))
                .build();

        return ValueProtos.TypedValue.newBuilder()
                .setType(
                        ValueProtos.Type.newBuilder()
                                .setListType(tupleListType)
                                .build()
                )
                .setValue(ids)
                .build();
    }

    @Override
    protected String outNames() {
        return resultSchema.flattenFields().stream()
                .map(this::getEscapedName)
                .collect(joining(", "));
    }

    private String getEscapedName(JavaField field) {
        return escape(field.getName());
    }

    @Override
    public String toDebugString(UpdateInStatementInput<T> in) {
        return String.format("updateIn(%s)", in);
    }

    private Set<String> nonNullFieldNames(Entity.Id<T> id) {
        return schema.flattenId(id).keySet();
    }

    @Value
    public static class UpdateInStatementInput<T extends Entity<T>> {
        Collection<? extends Entity.Id<T>> ids;
        Map<String, ?> values;
    }
}
