package tech.ydb.yoj.databind.expression;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.BooleanFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.ByteArrayFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.DateTimeFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.FlatFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.IntegerFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.RealFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.StringFieldExpected;
import tech.ydb.yoj.databind.expression.IllegalExpressionException.FieldTypeError.UnknownEnumConstant;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.stream;

@Getter
public final class ModelField {
    private final String userFieldPath;
    private final Schema.JavaField javaField;

    public ModelField(@Nullable String userFieldPath, @NonNull Schema.JavaField javaField) {
        this.userFieldPath = userFieldPath;
        this.javaField = javaField;
    }

    @NonNull
    public static ModelField of(@NonNull Schema.JavaField field) {
        return new ModelField(null, field);
    }

    @NonNull
    public Type getFlatFieldType() {
        checkState(javaField.isFlat(), FlatFieldExpected::new, p -> format("Not a flat field: \"%s\"", p));
        return javaField.getFlatFieldType();
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
        if (value.isTuple()) {
            value.getTuple().streamComponents()
                    .filter(jfv -> jfv.getValue() != null)
                    .forEach(jfv -> new ModelField(null, jfv.getField()).validateValue(FieldValue.ofObj(jfv.getValue(), jfv.getField())));
            return value;
        }

        Type fieldType = getFlatFieldType();
        FieldValueType fieldValueType = FieldValueType.forJavaType(fieldType, javaField.getField().getColumn());
        if (value.isString()) {
            if (fieldValueType == FieldValueType.ENUM) {
                TypeToken<?> tt = TypeToken.of(fieldType);
                String enumConstant = value.getStr();
                Class<?> clazz = tt.getRawType();
                checkArgument(enumHasConstant(clazz, enumConstant),
                        p -> new UnknownEnumConstant(p, enumConstant),
                        p -> format("Unknown enum constant for field \"%s\": \"%s\"", p, enumConstant));
            } else {
                checkArgument(fieldValueType == FieldValueType.STRING,
                        StringFieldExpected::new,
                        p -> format("Specified a string value for non-string field \"%s\"", p));
            }
        } else if (value.isNumber()) {
            checkArgument(
                    fieldValueType == FieldValueType.INTEGER,
                    IntegerFieldExpected::new,
                    p -> format("Specified an integer value for non-integer field \"%s\"", p));
        } else if (value.isReal()) {
            checkArgument(
                    fieldValueType == FieldValueType.REAL,
                    RealFieldExpected::new,
                    p -> format("Specified a real value for non-real field \"%s\"", p));
        } else if (value.isBool()) {
            checkArgument(fieldValueType == FieldValueType.BOOLEAN,
                    BooleanFieldExpected::new,
                    p -> format("Specified a boolean value for non-boolean field \"%s\"", p));
        } else if (value.isByteArray()) {
            checkArgument(fieldValueType == FieldValueType.BYTE_ARRAY,
                    ByteArrayFieldExpected::new,
                    p -> format("Specified a ByteArray value for non-ByteArray field \"%s\"", p));
        } else if (value.isTimestamp()) {
            checkArgument(fieldValueType == FieldValueType.TIMESTAMP || fieldValueType == FieldValueType.INTEGER,
                    DateTimeFieldExpected::new,
                    p -> format("Specified a timestamp value for non-timestamp field \"%s\"", p));
        } else {
            throw new UnsupportedOperationException("Unsupported field value type. This should never happen!");
        }
        return value;
    }

    private static boolean enumHasConstant(@NonNull Class<?> enumClass, @NonNull String enumConstant) {
        return stream(enumClass.getEnumConstants()).anyMatch(c -> enumConstant.equals(((Enum<?>) c).name()));
    }

    public <U> ModelField forSchema(@NonNull Schema<U> dstSchema,
                                    @NonNull UnaryOperator<String> pathTransformer) {
        String newFieldPath = pathTransformer.apply(javaField.getPath());
        Schema.JavaField newField = dstSchema.getField(newFieldPath);
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

    private void checkArgument(boolean condition,
                               Function<String, ? extends IllegalExpressionException> userException,
                               Function<String, String> internalErrorMessage) {
        ensure(condition, userException, p -> new IllegalArgumentException(internalErrorMessage.apply(p)));
    }

    private void checkState(boolean condition,
                            Function<String, ? extends IllegalExpressionException> userException,
                            Function<String, String> internalErrorMessage) {
        ensure(condition, userException, p -> new IllegalStateException(internalErrorMessage.apply(p)));
    }
}
