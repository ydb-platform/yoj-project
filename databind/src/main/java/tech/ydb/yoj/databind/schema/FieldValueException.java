package tech.ydb.yoj.databind.schema;

import javax.annotation.Nullable;

import static java.lang.String.format;

public final class FieldValueException extends BindingException {
    public FieldValueException(@Nullable Throwable cause, String fieldName, Object containingObject) {
        super(cause, ex -> message(fieldName, containingObject, ex));
    }

    private static String message(String fieldName, Object obj, Throwable ex) {
        String safeObj;
        try {
            safeObj = String.valueOf(obj);
        } catch (Exception toStrEx) {
            safeObj = format("[value of %s; threw on toString(): %s]", obj.getClass(), toStrEx);
        }

        return format("Could not get value of field \"%s\" from %s%s", fieldName, safeObj, ex == null ? "" : ": " + ex);
    }
}
