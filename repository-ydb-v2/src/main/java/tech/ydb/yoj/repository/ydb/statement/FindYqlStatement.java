package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.util.lang.Strings.lazyDebugMsg;

public class FindYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT> extends YqlStatement<PARAMS, ENTITY, RESULT> {
    public FindYqlStatement(
            TableDescriptor<ENTITY> tableDescriptor, EntitySchema<ENTITY> schema, Schema<RESULT> resultSchema
    ) {
        super(tableDescriptor, schema, resultSchema);
    }

    @Override
    public List<YqlStatementParam> getParams() {
        return schema.flattenId().stream()
                .map(c -> YqlStatementParam.required(YqlType.of(c), c.getName()))
                .collect(toList());
    }

    @Override
    public String getQuery(String tablespace) {
        return declarations()
                + "SELECT " + outNames()
                + " FROM " + table(tablespace)
                + " WHERE " + nameEqVars();
    }

    @Override
    public List<RESULT> readFromCache(PARAMS params, RepositoryCache cache) {
        RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), tableDescriptor, params);
        if (!cache.contains(key)) {
            return null;
        }

        //noinspection unchecked
        return cache.get(key)
                .map(o -> Collections.singletonList((RESULT) o))
                .orElse(Collections.emptyList());
    }

    @Override
    public void storeToCache(PARAMS params, List<RESULT> result, RepositoryCache cache) {
        RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), tableDescriptor, params);
        cache.put(key, result.stream().findFirst().orElse(null));
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT;
    }

    @Override
    public Object toDebugString(PARAMS params) {
        return lazyDebugMsg("find(%s)", params);
    }
}
