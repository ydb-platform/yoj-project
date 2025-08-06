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
import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

/**
 * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
 * have specific requirements.
 * <ul>
 * <li>Blind updates disrupt query merging mechanism, so you won't able to run multiple blind update statements in the same transaction,
 * or interleave them with upserts ({@code Table.save()}) and inserts ({@code Table.insert()}).</li>
 * <li>Blind updates manipulate the database columns directly and do not load the entity before performing the update, so:
 *   <ul>
 *     <li>Blind updates will <strong>easily break entity invariants</strong>, and an entity instance might not even be possible to read back after
 *     a blind update.</li>
 *     <li>{@code Entity.preSave()} will never be invoked on a blind update.</li>
 *     <li>Entity projections may become inconsistent with the main entity, if the blind update changed fields that are used by entity projections.
 *     Projections will become an optional library feature in YOJ 3.0.0, so this concern will likely be less important for users of YOJ core
 *     (without the projections).</li>
 *   </ul>
 * </li>
 * <li>Blind updates remove the entity from the first-level cache (to force a re-read from the database), but this too comes with caveats:
 *   <ul>
 *     <li>Reads by {@code ID/Set<ID>} in the transaction will <strong>behave unintuitively in the default <em>delayed writes</em> mode:</strong>
 * we will return the <strong>unpatched</strong> entity, because blind update is only executed right before transaction commit.</li>
 *     <li>If you absolutely need to read back patched entity, you have to <strong>enable <em>immediate writes</em></strong>, which will execute
 * write statements right away. You won't get performance benefits of the delayed writes mode anyway, because blind updates disrupt query merging...
 *     </li>
 *   </ul>
 * </ul>
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
    public Object toDebugString(UpdateModel.ById<ID> idById) {
        return lazyDebugMsg("updateById(%s)", idById.getId());
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
