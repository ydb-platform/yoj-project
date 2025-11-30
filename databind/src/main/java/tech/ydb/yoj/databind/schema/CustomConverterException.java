package tech.ydb.yoj.databind.schema;

import tech.ydb.yoj.ExperimentalApi;

import javax.annotation.Nullable;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/24")
public final class CustomConverterException extends BindingException {
    public CustomConverterException(@Nullable Throwable cause, String message) {
        super(cause, __ -> message);
    }
}
