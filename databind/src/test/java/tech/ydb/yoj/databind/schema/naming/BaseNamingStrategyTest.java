package tech.ydb.yoj.databind.schema.naming;

import tech.ydb.yoj.databind.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseNamingStrategyTest {
    protected abstract <T> Schema<T> getSchema(Class<T> entityType);

    protected <T> void verifyTableName(Class<T> entityType, String tableName) {
        Schema<T> schema = getSchema(entityType);

        assertThat(schema.getName()).isEqualTo(tableName);
    }

    protected <T> void verifyFieldNames(Class<T> entityType, String... names) {
        Schema<T> schema = getSchema(entityType);

        assertThat(schema.flattenFieldNames()).containsOnly(names);
    }
}
