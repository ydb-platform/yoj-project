package tech.ydb.yoj.repository.ydb.statement;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;

class DeleteAllStatement<PARAMS, ENTITY extends Entity<ENTITY>> extends YqlStatement<PARAMS, ENTITY, ENTITY> {
    public DeleteAllStatement(@NonNull Class<ENTITY> type) {
        super(EntitySchema.of(type), EntitySchema.of(type));
    }

    public DeleteAllStatement(@NonNull EntitySchema<ENTITY> schema, String tableName) {
        super(schema, schema, tableName);
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
        return "deleteAll(" + schema.getName() + ")";
    }
}
