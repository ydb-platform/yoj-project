package tech.ydb.yoj.repository.ydb.statement;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlOrderBy;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

public class FindStatement<ENTITY extends Entity<ENTITY>, RESULT> extends PredicateStatement<Collection<? extends YqlStatementPart<?>>, ENTITY, RESULT> {
    private final boolean distinct;
    private final List<YqlStatementPart<?>> parts;

    public static <E extends Entity<E>, R> FindStatement<E, R> from(
            @NonNull EntitySchema<E> schema,
            @NonNull Schema<R> outSchema,
            @NonNull Collection<? extends YqlStatementPart<?>> parts,
            boolean distinct
    ) {
        ArrayList<YqlStatementPart<?>> partsList = new ArrayList<>(parts);
        if (!distinct) {
            if (parts.stream().noneMatch(s -> s.getType().equals(YqlOrderBy.TYPE))) {
                partsList.add(ORDER_BY_ID_ASCENDING);
            }
        }

        return new FindStatement<>(schema, outSchema, partsList, distinct);
    }

    private FindStatement(
            @NonNull EntitySchema<ENTITY> schema,
            @NonNull Schema<RESULT> outSchema,
            @NonNull List<YqlStatementPart<?>> parts,
            boolean distinct
    ) {
        super(schema, outSchema, parts, YqlStatement::predicateFrom);
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
