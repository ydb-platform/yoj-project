package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.repository.db.Entity;

import java.util.Map;
import java.util.function.Function;

public class InsertYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>> extends MultipleVarsYqlStatement.Simple<PARAMS, ENTITY> {
    public InsertYqlStatement(Class<ENTITY> type) {
        super(type);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.INSERT;
    }

    @Override
    public String toDebugString(PARAMS params) {
        return "insert(" + toDebugParams(params) + ")";
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations() +
                "INSERT INTO " + table(tablespace) + " SELECT * FROM AS_TABLE(" + listName + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Function<PARAMS, Map<String, Object>> flattenInputVariables() {
        return t -> schema.flatten((ENTITY) t);
    }
}
