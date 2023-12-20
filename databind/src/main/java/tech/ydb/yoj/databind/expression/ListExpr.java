package tech.ydb.yoj.databind.expression;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(access = PRIVATE)
public class ListExpr<T> extends LeafExpression<T> {
    Schema<T> schema;

    boolean generated;

    Operator operator;

    Schema.JavaField field;

    List<FieldValue> values;

    public ListExpr(@NonNull Schema<T> schema, boolean generated,
                    @NonNull ModelField field, @NonNull Operator operator, @NonNull List<FieldValue> values) {
        this(schema, generated, operator, field.getJavaField(), values.stream().map(field::validateValue).collect(toList()));
    }

    @Override
    public FilterExpression.Type getType() {
        return Type.LIST;
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

    @Nullable
    public Comparable<?> getActualValue(@NonNull T obj) {
        return FieldValue.getComparable(schema.flatten(obj), field);
    }

    @NonNull
    public List<Comparable<?>> getExpectedValues() {
        java.lang.reflect.Type fieldType = getFieldType();
        return values.stream().map(v -> v.getComparable(fieldType)).collect(toList());
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitListExpr(this);
    }

    @Override
    public FilterExpression<T> negate() {
        return new ListExpr<>(schema, generated, operator.negate(), field, values);
    }

    @Override
    public <U> ListExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        Schema.JavaField newField = this.field.forSchema(dstSchema, pathTransformer);
        return new ListExpr<>(dstSchema, this.generated, this.operator, newField, this.values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ListExpr<?> listExpr = (ListExpr<?>) o;

        if (!schema.equals(listExpr.schema)) {
            return false;
        }
        if (!operator.name().equals(listExpr.operator.name())) {
            return false;
        }
        if (!field.equals(listExpr.field)) {
            return false;
        }
        return values.equals(listExpr.values);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + operator.name().hashCode();
        result = 31 * result + field.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s %s(%s)", field.getPath(), operator, values.stream().map(Object::toString).collect(joining(", ")));
    }

    public enum Operator {
        IN {
            @Override
            public Operator negate() {
                return NOT_IN;
            }

            @Override
            public String toString() {
                return "IN";
            }
        },
        NOT_IN {
            @Override
            public Operator negate() {
                return IN;
            }

            @Override
            public String toString() {
                return "NOT IN";
            }
        };

        public abstract Operator negate();
    }
}
