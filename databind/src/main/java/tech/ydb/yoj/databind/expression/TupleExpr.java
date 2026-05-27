package tech.ydb.yoj.databind.expression;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.expression.values.FieldValue;
import tech.ydb.yoj.databind.expression.values.Tuple;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@Value
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
public class TupleExpr<T> extends LeafExpression<T> {
    Schema<T> schema;

    boolean generated;

    Operator operator;

    List<Tuple.FieldAndValue> values;

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    public TupleExpr(
            @NonNull Schema<T> schema, boolean generated, Operator operator,
            @NonNull List<ModelField> fields, List<?> values
    ) {
        this(schema, generated, operator, fieldsAndValues(fields, values));
    }

    private static List<Tuple.FieldAndValue> fieldsAndValues(@NonNull List<ModelField> fields, @NonNull List<?> values) {
        Preconditions.checkArgument(!fields.isEmpty(), "Tuple field list must not be empty");
        Preconditions.checkArgument(!values.isEmpty(), "Tuple field values list must not be empty");

        List<Tuple.FieldAndValue> fieldsAndValues = new ArrayList<>();

        int index = 0;
        Set<ModelField> allFields = new HashSet<>();
        Iterator<ModelField> fieldIterator = fields.iterator();
        Iterator<?> valueIterator = values.iterator();
        while (fieldIterator.hasNext() && valueIterator.hasNext()) {
            ModelField field = fieldIterator.next();
            Preconditions.checkArgument(field != null, "Tuple field (at index %s) must not be null", index);
            Preconditions.checkArgument(allFields.add(field),
                    "Tuple field (at index %s) must be unique, but got '%s' multiple times", field, index);

            Object value = valueIterator.next();
            Preconditions.checkArgument(value != null,
                    "Tuple value for field '%s' (at index %s) must not be null", field, index);

            fieldsAndValues.add(fieldAndValue(field, value));

            index++;
        }
        Preconditions.checkArgument(!fieldIterator.hasNext(), "Got more fields in a tuple than values (> %s)", index);
        Preconditions.checkArgument(!valueIterator.hasNext(), "Got more values in a tuple than fields (> %s)", index);

        return Collections.unmodifiableList(fieldsAndValues);
    }

    private static Tuple.FieldAndValue fieldAndValue(@NonNull ModelField field, @NonNull Object value) {
        Schema.JavaField javaField = field.getJavaField().toFlatField();
        return new Tuple.FieldAndValue(
                javaField,
                field.validateValue(FieldValue.ofObj(value, javaField)),
                value
        );
    }

    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
    private TupleExpr(
            @NonNull Schema<T> schema, boolean generated, @NonNull Operator operator,
            @NonNull List<Tuple.FieldAndValue> values
    ) {
        this.schema = schema;
        this.generated = generated;
        this.operator = operator;

        Preconditions.checkArgument(!values.isEmpty(), "Tuple field value list must not be empty");
        this.values = List.copyOf(values);
    }

    @Override
    public Type getType() {
        return Type.TUPLE;
    }

    @NonNull
    public List<String> getFieldPaths() {
        return values.stream().map(fv -> fv.field().getPath()).toList();
    }

    @NonNull
    public Tuple getActualTuple(@NonNull T obj) {
        Map<String, Object> actual = schema.flatten(obj);
        List<Tuple.FieldAndValue> actualValues = new ArrayList<>(values.size());
        for (Tuple.FieldAndValue fv : values) {
            actualValues.add(new Tuple.FieldAndValue(fv.field(), actual));
        }
        return new Tuple(null, actualValues, null);
    }

    @NonNull
    public Tuple getExpectedTuple() {
        return new Tuple(null, values, null);
    }

    @Override
    public <U> FilterExpression<U> forSchema(@NonNull Schema<U> dstSchema, @NonNull UnaryOperator<String> pathTransformer) {
        return new TupleExpr<>(dstSchema, this.generated, this.operator, this.values.stream()
                .map(fv -> new Tuple.FieldAndValue(
                        fv.field().forSchema(dstSchema, pathTransformer),
                        fv.value(),
                        fv.rawValue()
                ))
                .toList());
    }

    @Override
    public <V> V visit(@NonNull Visitor<T, V> visitor) {
        return visitor.visitTupleExpr(this);
    }

    @Override
    public FilterExpression<T> negate() {
        Operator negation = operator.negate();
        return negation != null ? new TupleExpr<>(schema, generated, negation, values) : new NotExpr<>(schema, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TupleExpr<?> that = (TupleExpr<?>) o;

        if (!schema.equals(that.schema)) {
            return false;
        }
        if (!operator.name().equals(that.operator.name())) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + operator.name().hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("(%s) %s (%s)",
                values.stream().map(fv -> fv.field().getPath()).collect(joining(", ")),
                operator,
                values.stream().map(fv -> String.valueOf(fv.value())).collect(joining(", "))
        );
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
        };

        @Nullable
        public abstract Operator negate();
    }
}
