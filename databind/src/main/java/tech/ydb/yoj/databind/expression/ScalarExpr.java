package tech.ydb.yoj.databind.expression;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(access = PRIVATE)
public class ScalarExpr<T> extends LeafExpression<T> {
    Schema<T> schema;

    boolean generated;

    Operator operator;

    @Getter(PRIVATE)
    Schema.JavaField field;

    FieldValue value;

    public ScalarExpr(@NonNull Schema<T> schema, boolean generated,
                      @NonNull ModelField field, @NonNull Operator operator, @NonNull FieldValue value) {
        this(schema, generated, operator, field.getJavaField(), field.validateValue(value));
    }

    @Override
    public FilterExpression.Type getType() {
        return Type.SCALAR;
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
    public Comparable<?> getExpectedValue() {
        return value.getComparable(getFieldType(), getColumnAnnotation());
    }

    @Nullable
    @Override
    public Column getColumnAnnotation() {
        return field.getField().getColumn();
    }

    @NonNull
    @Override
    public java.lang.reflect.Type getFieldType() {
        return getField().isFlat() ? getField().getFlatFieldType() : getField().getType();
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitScalarExpr(this);
    }

    @Override
    public FilterExpression<T> negate() {
        return new ScalarExpr<>(schema, generated, operator.negate(), field, value);
    }

    @Override
    public <U> ScalarExpr<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        Schema.JavaField newField = this.field.forSchema(dstSchema, pathTransformer);
        return new ScalarExpr<>(dstSchema, this.generated, this.operator, newField, this.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScalarExpr<?> that = (ScalarExpr<?>) o;

        if (!schema.equals(that.schema)) {
            return false;
        }
        if (!operator.name().equals(that.operator.name())) {
            return false;
        }
        if (!field.equals(that.field)) {
            return false;
        }
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + operator.name().hashCode();
        result = 31 * result + field.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s %s %s", field.getPath(), operator, value);
    }

    public enum Operator {
        /**
         * Exact match for numbers, case-sensitive match for strings.
         * E.g., {@code zone='ru-central-1'} or {@code size=1024}
         */
        EQ {
            @Override
            public Operator negate() {
                return NEQ;
            }

            @Override
            public String toString() {
                return "==";
            }
        },
        /**
         * Exact negative match for numbers, case-sensitive negative match for strings.
         * E.g., {@code zone!="ru-central-1"} or {@code size!=1024}
         */
        NEQ {
            @Override
            public Operator negate() {
                return EQ;
            }

            @Override
            public String toString() {
                return "!=";
            }
        },
        /**
         * "Less than" match for numbers and strings (strings are compared lexicographically).
         * E.g., {@code size < 100}
         */
        LT {
            @Override
            public Operator negate() {
                return GTE;
            }

            @Override
            public String toString() {
                return "<";
            }
        },
        /**
         * "Less than or equal" match for numbers and strings (strings are compared lexicographically).
         * E.g., {@code size <= 2048}
         */
        LTE {
            @Override
            public Operator negate() {
                return GT;
            }

            @Override
            public String toString() {
                return "<=";
            }
        },
        /**
         * "Greater than" match for numbers and strings (strings are compared lexicographically).
         * E.g., {@code size > 100}
         */
        GT {
            @Override
            public Operator negate() {
                return LTE;
            }

            @Override
            public String toString() {
                return ">";
            }
        },
        /**
         * "Greater than or equal" match for numbers and strings (strings are compared lexicographically).
         * E.g., {@code size >= 4096}
         */
        GTE {
            @Override
            public Operator negate() {
                return LT;
            }

            @Override
            public String toString() {
                return ">=";
            }
        },
        /**
         * "Contains" case-sensitive match for a substring in a string
         * E.g., {@code name contains "abc"}
         */
        CONTAINS {
            @Override
            public Operator negate() {
                return NOT_CONTAINS;
            }

            @Override
            public String toString() {
                return "contains";
            }
        },
        /**
         * "Not contains" case-sensitive absence of a substring in a string
         * E.g., {@code name not contains "abc"}
         */
        NOT_CONTAINS {
            @Override
            public Operator negate() {
                return CONTAINS;
            }

            @Override
            public String toString() {
                return "not contains";
            }
        };

        public abstract Operator negate();
    }
}
