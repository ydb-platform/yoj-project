package tech.ydb.yoj.repository.ydb.statement;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.Collection;
import java.util.function.Function;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

public class FindStatement<ENTITY extends Entity<ENTITY>, RESULT> extends PredicateStatement<Collection<? extends YqlStatementPart<?>>, ENTITY, RESULT> {
    private final boolean distinct;
    private final Collection<? extends YqlStatementPart<?>> parts;

    public FindStatement(
            @NonNull EntitySchema<ENTITY> schema,
            @NonNull Schema<RESULT> outSchema,
            @NonNull Collection<? extends YqlStatementPart<?>> parts,
            @NonNull Function<Collection<? extends YqlStatementPart<?>>, YqlPredicate> predicateFrom,
            boolean distinct,
            String tableName) {
        super(schema, outSchema, parts, predicateFrom, tableName);
        this.distinct = distinct;
        this.parts = parts;
    }

    public String getQuery(String tablespace) {
        return declarations()
                + "SELECT " + (distinct ? "DISTINCT " : "") + outNames()
                + " FROM " + table(tablespace)
                + " " + mergeParts(parts.stream())
                .sorted(comparing(YqlStatementPart::getPriority))
                .map(sp -> sp.toFullYql(schema))
                .map(this::resolveParamNames)
                .collect(joining(" "));
    }

    @Override
    public Statement.QueryType getQueryType() {
        return Statement.QueryType.SELECT;
    }

    @Override
    public String toDebugString(Collection<? extends YqlStatementPart<?>> yqlStatementParts) {
        return "find(" + yqlStatementParts + ")";
    }
}
