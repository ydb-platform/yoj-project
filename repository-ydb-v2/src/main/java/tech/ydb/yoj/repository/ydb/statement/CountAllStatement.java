package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.Collection;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

public class CountAllStatement<ENTITY extends Entity<ENTITY>> extends PredicateStatement<Collection<? extends YqlStatementPart<?>>, ENTITY, Count> {
    private final List<YqlStatementPart<?>> parts;

    public CountAllStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, List<YqlStatementPart<?>> parts
    ) {
        super(tableDescriptor, schema, ObjectSchema.of(Count.class), parts, YqlPredicate::from);
        this.parts = parts;
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations()
                + "SELECT COUNT(*) AS count"
                + " FROM " + table(tablespace)
                + " " + mergeParts(parts)
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
    public Object toDebugString(Collection<? extends YqlStatementPart<?>> yqlStatementParts) {
        return parts.isEmpty() ? "count()" : lazyDebugMsg("count(%s)", parts);
    }
}
