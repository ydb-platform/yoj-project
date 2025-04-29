package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static java.util.stream.Stream.concat;

/**
 * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
 * have specific requirements.
 * <p>Blind updates disrupt query merging mechanism, so you typically won't able to run multiple blind update statements
 * in the same transaction, or interleave them with upserts ({@code Table.save()}) and inserts.
 * <p>Blind updates also do not update projections because they do not load the entity before performing the update;
 * this can cause projections to be inconsistent with the main entity.
 */
@Deprecated
public final class UpdateByIdStatement<ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>>
        extends YqlStatement<UpdateModel.ById<ID>, ENTITY, ENTITY> {
    private final Map<String, UpdateSetParam> setParams;

    private final Set<YqlStatementParam> idParams;

    public UpdateByIdStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, UpdateModel.ById<ID> model
    ) {
        super(tableDescriptor, schema, schema);
        this.idParams = schema.flattenId().stream()
                .map(c -> YqlStatementParam.required(YqlType.of(c), c.getName()))
                .collect(toUnmodifiableSet());
        this.setParams = UpdateSetParam.setParamsFromModel(schema, model).collect(toMap(UpdateSetParam::getName, p -> p));
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(UpdateModel.ById<ID> parameters) {
        Map<String, ValueProtos.TypedValue> queryParams = new LinkedHashMap<>();

        Map<String, ?> idValues = schema.flattenId(parameters.getId());
        idParams()
                .filter(p -> idValues.containsKey(p.getName()))
                .forEach(p -> queryParams.put(
                        p.getVar(),
                        createTQueryParameter(p.getType(), idValues.get(p.getName()), p.isOptional())
                ));

        setParams.forEach((name, param) -> queryParams.put(
                param.getVar(),
                createTQueryParameter(param.getType(), param.getFieldValue(parameters), param.isOptional())
        ));

        return unmodifiableMap(queryParams);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.UPDATE;
    }

    @Override
    public String toDebugString(UpdateModel.ById<ID> idById) {
        return "updateById(" + idById.getId() + ")";
    }

    @Override
    protected Collection<YqlStatementParam> getParams() {
        return concat(idParams(), setParams()).collect(toList());
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations()
                + "UPDATE " + table(tablespace) + " "
                + on() + " "
                + values();
    }

    private String on() {
        return "ON (" +
                concat(
                        idParams().map(YqlStatementParam::getName),
                        setParams().map(UpdateSetParam::getFieldName)
                ).collect(joining(", "))
                + ")";
    }

    private Stream<UpdateSetParam> setParams() {
        return setParams.values().stream();
    }

    private Stream<YqlStatementParam> idParams() {
        return idParams.stream();
    }

    private String values() {
        return "VALUES (" + concat(idParams(), setParams()).map(YqlStatementParam::getVar).collect(joining(", ")) + ")";
    }
}
