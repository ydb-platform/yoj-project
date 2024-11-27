package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.Collection;
import java.util.function.Function;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

class CountAllStatement<ENTITY extends Entity<ENTITY>> extends PredicateStatement<Collection<? extends YqlStatementPart<?>>, ENTITY, Count> {
    private final Collection<? extends YqlStatementPart<?>> parts;

    public CountAllStatement(
            EntitySchema<ENTITY> schema,
            Schema<Count> resultSchema,
            Collection<? extends YqlStatementPart<?>> parts,
            Function<Collection<? extends YqlStatementPart<?>>, YqlPredicate> predicateFrom
    ) {
        super(schema, resultSchema, parts, predicateFrom);
        this.parts = parts;
    }

    public CountAllStatement(
            EntitySchema<ENTITY> schema,
            Schema<Count> resultSchema,
            Collection<? extends YqlStatementPart<?>> parts,
            Function<Collection<? extends YqlStatementPart<?>>, YqlPredicate> predicateFrom,
            String tableName
    ) {
        super(schema, resultSchema, parts, predicateFrom, tableName);
        this.parts = parts;
    }

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
}
