package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

public class DeleteAllStatement<PARAMS, ENTITY extends Entity<ENTITY>> extends YqlStatement<PARAMS, ENTITY, ENTITY> {
    public DeleteAllStatement(TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema) {
        super(tableDescriptor, schema, schema);
    }

    @Override
    public String getQuery(String tablespace) {
        return "DELETE FROM " + table(tablespace);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.DELETE_ALL;
    }

    @Override
    public String toDebugString(PARAMS params) {
        return "deleteAll(" + tableDescriptor.toDebugString() + ")";
    }
}
