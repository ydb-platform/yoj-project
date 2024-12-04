package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.repository.db.EntityIdSchema.isIdField;

public abstract class MultipleVarsYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT> extends YqlStatement<PARAMS, ENTITY, RESULT> {
    public static final String listName = "$Input";

    public MultipleVarsYqlStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, Schema<RESULT> resultSchema
    ) {
        super(tableDescriptor, schema, resultSchema);
    }

    @Override
    protected String declarations() {
        String fieldPattern = escape("%s") + ":%s%s";
        String struct = getParams().stream()
                .map(p -> String.format(fieldPattern, p.getName(), p.getType().getYqlTypeName(), p.isOptional() ? "?" : ""))
                .collect(joining(","));
        return getDeclaration(listName, "List<Struct<" + struct + ">>");
    }

    @Override
    public List<YqlStatementParam> getParams() {
        return schema.flattenFields().stream()
                .map(c -> new YqlStatementParam(YqlType.of(c), c.getName(), isOptional(c)))
                .collect(toList());
    }

    private static boolean isOptional(Schema.JavaField f) {
        return !isIdField(f);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, ValueProtos.TypedValue> toQueryParameters(PARAMS params) {
        List<YqlStatementParam> yqlParams = getParams();
        ValueProtos.StructType.Builder structTypeBuilder = yqlParams.stream()
                .map(p -> ValueProtos.StructMember.newBuilder().setName(p.getName()).setType(getYqlType(p.getType(), p.isOptional())))
                .collect(Collector.of(
                        ValueProtos.StructType::newBuilder,
                        ValueProtos.StructType.Builder::addMembers,
                        (b1, b2) -> b1.addAllMembers(b2.getMembersList())
                ));
        ValueProtos.Value.Builder structBuilder = (params instanceof Collection
                ? ((Collection<PARAMS>) params)
                : singleton(params)).stream()
                .map(flattenInputVariables())
                .map(fieldValues -> yqlParams.stream()
                        .map(p -> getYqlValue(p.getType(), fieldValues.get(p.getName())))
                        .collect(itemsCollector))
                .collect(itemsCollector);
        return singletonMap(
                listName,
                ValueProtos.TypedValue.newBuilder()
                        .setType(ValueProtos.Type.newBuilder()
                                .setListType(ValueProtos.ListType.newBuilder()
                                        .setItem(ValueProtos.Type.newBuilder().setStructType(structTypeBuilder))
                                        .build())
                                .build())
                        .setValue(structBuilder)
                        .build()
        );
    }

    protected abstract Function<PARAMS, Map<String, Object>> flattenInputVariables();

    protected String toDebugParams(PARAMS params) {
        if (params instanceof Collection<?> c) {
            return switch (c.size()) {
                case 0 -> "[]";
                case 1 -> "[" + c.iterator().next() + "]";
                default -> "[" + c.iterator().next() + ",...](" + c.size() + ")";
            };
        }
        return String.valueOf(params);
    }

    public abstract static class Simple<PARAMS, ENTITY extends Entity<ENTITY>>
            extends MultipleVarsYqlStatement<PARAMS, ENTITY, ENTITY> {
        public Simple(TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema) {
            super(tableDescriptor, schema, schema);
        }
    }
}
