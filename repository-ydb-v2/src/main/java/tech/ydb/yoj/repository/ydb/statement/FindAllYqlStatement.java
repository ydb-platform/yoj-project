package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

public class FindAllYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT> extends YqlStatement<PARAMS, ENTITY, RESULT> {

    public FindAllYqlStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, Schema<RESULT> resultSchema
    ) {
        super(tableDescriptor, schema, resultSchema);
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations()
                + "SELECT " + outNames()
                + " FROM " + table(tablespace)
                + " " + ORDER_BY_ID_ASCENDING.toFullYql(schema);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT;
    }

    @Override
    public String toDebugString(PARAMS params) {
        return "findAll(" + schema.getName() + ")";
    }
}
