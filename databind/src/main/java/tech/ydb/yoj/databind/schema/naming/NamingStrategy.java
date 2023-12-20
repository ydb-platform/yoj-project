package tech.ydb.yoj.databind.schema.naming;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

public interface NamingStrategy {
    String NAME_DELIMITER = "_";

    String getNameForClass(@NonNull Class<?> entityClass);

    /**
     * Assigns a name to a field in a schema.
     */
    void assignFieldName(@NonNull JavaField javaField);
}
