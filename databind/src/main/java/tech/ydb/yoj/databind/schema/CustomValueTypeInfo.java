package tech.ydb.yoj.databind.schema;

import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

@Value
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public class CustomValueTypeInfo<J, C extends Comparable<? super C>> {
    @NonNull
    Class<C> columnClass;

    @NonNull
    ValueConverter<J, C> converter;

    @NonNull
    public J toJava(@NonNull JavaField field, @NonNull C column) {
        return converter.toJava(field, column);
    }

    @NonNull
    public C toColumn(@NonNull JavaField field, @NonNull J java) {
        return converter.toColumn(field, java);
    }
}
