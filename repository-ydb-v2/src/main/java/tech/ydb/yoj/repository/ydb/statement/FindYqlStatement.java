package tech.ydb.yoj.repository.ydb.statement;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class FindYqlStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT> extends YqlStatement<PARAMS, ENTITY, RESULT> {
    public FindYqlStatement(@NonNull EntitySchema<ENTITY> schema, @NonNull Schema<RESULT> resultSchema) {
        super(schema, resultSchema);
    }

    public FindYqlStatement(@NonNull EntitySchema<ENTITY> schema, @NonNull Schema<RESULT> resultSchema, String tableName) {
        super(schema, resultSchema, tableName);
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
        RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), params);
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
        RepositoryCache.Key key = new RepositoryCache.Key(resultSchema.getType(), params);
        cache.put(key, result.stream().findFirst().orElse(null));
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT;
    }

    @Override
    public String toDebugString(PARAMS params) {
        return "find(" + params + ")";
    }
}
