package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import java.util.Map;
import java.util.function.Function;

import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

public class InsertYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>> extends MultipleVarsYqlStatement.Simple<PARAMS, ENTITY> {
    public InsertYqlStatement(TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema) {
        super(tableDescriptor, schema);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.INSERT;
    }

    @Override
    public Object toDebugString(PARAMS params) {
        return lazyDebugMsg("insert(%s)", toDebugParams(params));
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
