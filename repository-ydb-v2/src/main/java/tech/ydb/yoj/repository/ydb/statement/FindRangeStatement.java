package tech.ydb.yoj.repository.ydb.statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
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

    private Stream<YqlStatementRangeParam> toParams(Set<JavaField> fields, FindRangeStatement.RangeBound rangeBound) {
        return schema.flattenId().stream()
                .filter(fields::contains)
                .map(c -> new FindRangeStatement.YqlStatementRangeParam(YqlType.of(c), c, rangeBound));
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(Range<ID> parameters) {
        return getParams().stream()
                .map(YqlStatementRangeParam.class::cast)
                .collect(toMap(
                        YqlStatementParam::getVar,
                        p -> createTQueryParameter(p.getType(), p.rangeBound.map(parameters).get(p.field), p.isOptional()))
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
                .map(p -> "(" + escape(p.field.getName()) + p.rangeBound.op + p.getVar() + ")")
                .collect(joining(" AND "));
    }

    @RequiredArgsConstructor
    private enum RangeBound {
        EQ("=", Range::getEqMap),
        MAX("<=", Range::getMaxMap),
        MIN(">=", Range::getMinMap);

        private final String op;
        private final Function<Range<?>, Map<JavaField, Object>> mapper;

        public Map<JavaField, Object> map(Range<?> range) {
            return mapper.apply(range);
        }
    }

    private static final class YqlStatementRangeParam extends YqlStatementParam {
        private final RangeBound rangeBound;
        private final JavaField field;

        private YqlStatementRangeParam(YqlType type, JavaField field, RangeBound rangeBound) {
            super(type, rangeBound.name() + "_" + field.getName(), true);
            this.rangeBound = rangeBound;
            this.field = field;
        }
    }
}
