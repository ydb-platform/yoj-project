package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.Map;
import java.util.function.Function;

public class UpsertYqlStatement<IN, T extends Entity<T>> extends MultipleVarsYqlStatement.Simple<IN, T> {
    public UpsertYqlStatement(TableDescriptor<T> tableDescriptor, EntitySchema<T> schema) {
        super(tableDescriptor, schema);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.UPSERT;
    }

    @Override
    public String toDebugString(IN in) {
        return "upsert(" + toDebugParams(in) + ")";
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations() +
                "UPSERT INTO " + table(tablespace) + " SELECT * FROM AS_TABLE(" + listName + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Function<IN, Map<String, Object>> flattenInputVariables() {
        return t -> schema.flatten((T) t);
    }
}
