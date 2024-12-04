package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

import java.util.Map;
import java.util.function.Function;

public class DeleteByIdStatement<IN, T extends Entity<T>> extends MultipleVarsYqlStatement.Simple<IN, T> {
    public DeleteByIdStatement(EntitySchema<T> schema) {
        super(schema);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.DELETE;
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations() +
                "DELETE FROM " + table(tablespace) + " ON SELECT * FROM AS_TABLE(" + listName + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Function<IN, Map<String, Object>> flattenInputVariables() {
        return t -> schema.flattenId((Entity.Id<T>) t);
    }

    @Override
    public String toDebugString(IN in) {
        return "delete(" + toDebugParams(in) + ")";
    }
}
