package tech.ydb.yoj.repository.ydb.statement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class FindRangeStatement<ENTITY extends Entity<ENTITY>, ID extends Entity.Id<ENTITY>, RESULT> extends YqlStatement<Range<ID>, ENTITY, RESULT> {
    @Getter
    private final List<YqlStatementParam> params;

    public FindRangeStatement(
            TableDescriptor<ENTITY> tableDescriptor,
            EntitySchema<ENTITY> schema,
            Schema<RESULT> outSchema,
            Range<ID> range
    ) {
        super(tableDescriptor, schema, outSchema);
        this.params = Stream.of(RangeBound.values())
                .flatMap(b -> toParams(b.map(range).keySet(), b))
                .collect(toList());
    }

    private Stream<YqlStatementRangeParam> toParams(Set<String> names, FindRangeStatement.RangeBound rangeBound) {
        return schema.flattenId().stream()
                .filter(f -> names.contains(f.getName()))
                .map(c -> new FindRangeStatement.YqlStatementRangeParam(YqlType.of(c), c.getName(), rangeBound));
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(Range<ID> parameters) {
        return getParams().stream()
                .map(YqlStatementRangeParam.class::cast)
                .collect(toMap(
                        YqlStatementParam::getVar,
                        p -> createTQueryParameter(p.getType(), p.rangeBound.map(parameters).get(p.rangeName), p.isOptional()))
                );
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT;
    }

    @Override
    public String toDebugString(Range<ID> idRange) {
        return "find(" + idRange + ")";
    }

    @Override
    public String getQuery(String tablespace) {
        String where = predicationVars();
        return declarations()
                + "SELECT " + outNames() + " FROM " + table(tablespace)
                + (where.isEmpty() ? "" : " WHERE " + where)
                + " " + ORDER_BY_ID_ASCENDING.toFullYql(schema);
    }

    private String predicationVars() {
        return getParams().stream()
                .map(YqlStatementRangeParam.class::cast)
                .map(p -> "(" + escape(p.rangeName) + p.rangeBound.op + p.getVar() + ")")
                .collect(joining(" AND "));
    }

    @AllArgsConstructor
    enum RangeBound {
        EQ("=", Range::getEqMap),
        MAX("<=", Range::getMaxMap),
        MIN(">=", Range::getMinMap);
        private final String op;
        private final Function<Range<?>, Map<String, Object>> mapper;

        public Map<String, Object> map(Range<?> range) {
            return mapper.apply(range);
        }
    }

    private static class YqlStatementRangeParam extends YqlStatementParam {
        private final RangeBound rangeBound;
        private final String rangeName;

        YqlStatementRangeParam(YqlType type, String name, RangeBound rangeBound) {
            // YqlStatementRangeParam is always about the value of ID field,
            // and YOJ disallows writing NULL to columns corresponding to ID fields.
            // ==> YqlStatementRangeParams are always required, never optional
            super(type, rangeBound.name() + "_" + name, false);
            this.rangeBound = rangeBound;
            this.rangeName = name;
        }
    }
}
