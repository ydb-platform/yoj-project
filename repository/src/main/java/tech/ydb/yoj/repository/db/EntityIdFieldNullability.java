package tech.ydb.yoj.repository.db;

import tech.ydb.yoj.InternalApi;

@InternalApi
/*package*/ enum EntityIdFieldNullability {
    ALWAYS_NULL,
    USE_COLUMN_ANNOTATION,
    ALWAYS_NOT_NULL;

    private static final String PROP_NOT_NULL_MODE = "tech.ydb.yoj.repository.db.idFieldNullability";

    public static EntityIdFieldNullability get() {
        return valueOf(System.getProperty(PROP_NOT_NULL_MODE, USE_COLUMN_ANNOTATION.name()));
    }
}
