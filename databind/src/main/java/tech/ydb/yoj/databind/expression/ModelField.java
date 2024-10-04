package tech.ydb.yoj.databind.expression;

import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.FlatFieldExpected;
import tech.ydb.yoj.databind.expression.values.FieldValue;
import tech.ydb.yoj.databind.expression.values.FieldValue.ValidationResult;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.String.format;

@Getter
public final class ModelField {
    private final String userFieldPath;
    private final JavaField javaField;

    public ModelField(@Nullable String userFieldPath, @NonNull JavaField javaField) {
        this.userFieldPath = userFieldPath;
        this.javaField = javaField;
    }

    @NonNull
    public static ModelField of(@NonNull JavaField field) {
        return new ModelField(null, field);
    }

    @NonNull
    public Type getFlatFieldType() {
        return toFlatField().getType();
    }

    private JavaField toFlatField() {
        checkState(javaField.isFlat(), FlatFieldExpected::new, p -> format("Not a flat field: \"%s\"", p));
        return javaField.toFlatField();
    }

    @NonNull
    public String getName() {
        return javaField.getName();
    }

    @NonNull
    public String getPath() {
        return javaField.getPath();
    }

    @NonNull
    public Stream<ModelField> flatten() {
        return javaField.flatten().map(f -> new ModelField(userFieldPath, f));
    }

    @NonNull
    public FieldValue validateValue(@NonNull FieldValue value) {
        JavaField field = javaField.isFlat() ? javaField.toFlatField() : javaField;
        FieldValueType fieldValueType = FieldValueType.forSchemaField(field);
        ValidationResult validationResult = value.isValidValueOfType(field.getType(), fieldValueType);
        if (validationResult.invalid()) {
            throw userFieldPath != null
                    ? validationResult.throwInternalError(userFieldPath)
                    : validationResult.throwUserException(javaField.getPath());
        }
        return value;
    }

    public <U> ModelField forSchema(@NonNull Schema<U> dstSchema,
                                    @NonNull UnaryOperator<String> pathTransformer) {
        String newFieldPath = pathTransformer.apply(javaField.getPath());
        JavaField newField = dstSchema.getField(newFieldPath);
        return new ModelField(this.userFieldPath, newField);
    }

    @NonNull
    @Override
    public String toString() {
        return userFieldPath != null ? userFieldPath : javaField.getPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ModelField that = (ModelField) o;
        return javaField.equals(that.javaField);
    }

    @Override
    public int hashCode() {
        return javaField.hashCode();
    }

    private void ensure(boolean condition,
                        Function<String, ? extends IllegalExpressionException> userException,
                        Function<String, ? extends RuntimeException> internalError) {
        if (!condition) {
            throw userFieldPath != null ? userException.apply(userFieldPath) : internalError.apply(javaField.getPath());
        }
    }

    private void checkState(boolean condition,
                            Function<String, ? extends IllegalExpressionException> userException,
                            Function<String, String> internalErrorMessage) {
        ensure(condition, userException, p -> new IllegalStateException(internalErrorMessage.apply(p)));
    }
}
