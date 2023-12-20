package tech.ydb.yoj.databind.expression;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.Map;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(access = PRIVATE)
public class NullExpr<T> extends LeafExpression<T> {
    Schema<T> schema;

    boolean generated;

    Operator operator;

    @Getter(PRIVATE)
    Schema.JavaField field;

    public NullExpr(@NonNull Schema<T> schema, boolean generated,
                    @NonNull ModelField field, @NonNull Operator operator) {
        this(schema, generated, operator, field.getJavaField());
    }

    @Override
    public Type getType() {
        return Type.NULL;
    }

    @Override
    public java.lang.reflect.Type getFieldType() {
        return getField().isFlat() ? getField().getFlatFieldType() : getField().getType();
    }

    @Override
    public String getFieldName() {
        return getField().getName();
    }

    @Override
    public String getFieldPath() {
        return getField().getPath();
    }

    public boolean isActualValueNull(@NonNull T obj) {
        Map<String, Object> flattened = schema.flatten(obj);
        return field.flatten().allMatch(f -> flattened.get(field.getName()) == null);
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitNullExpr(this);
    }

    @Override
    public FilterExpression<T> negate() {
        return new NullExpr<>(schema, generated, operator.negate(), field);
    }

    @Override
    public <U> NullExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        Schema.JavaField newField = this.field.forSchema(dstSchema, pathTransformer);
        return new NullExpr<>(dstSchema, this.generated, this.operator, newField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NullExpr<?> that = (NullExpr<?>) o;

        if (!schema.equals(that.schema)) {
            return false;
        }
        if (!operator.name().equals(that.operator.name())) {
            return false;
        }
        return field.equals(that.field);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + operator.name().hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s %s", field.getPath(), operator);
    }

    public enum Operator {
        /**
         * The value is null.
         */
        IS_NULL {
            @Override
            public Operator negate() {
                return IS_NOT_NULL;
            }

            @Override
            public String toString() {
                return "IS NULL";
            }
        },
        /**
         * The value is not null.
         */
        IS_NOT_NULL {
            @Override
            public Operator negate() {
                return IS_NULL;
            }

            @Override
            public String toString() {
                return "IS NOT NULL";
            }
        };

        public abstract Operator negate();
    }
}
