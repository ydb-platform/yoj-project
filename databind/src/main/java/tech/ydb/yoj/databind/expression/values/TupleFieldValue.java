package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.expression.FieldValue;

import java.lang.reflect.Type;
import java.util.Objects;

public record TupleFieldValue(
        Tuple tuple
) implements FieldValue {
    @Override
    public Comparable<?> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.COMPOSITE) {
            throw new IllegalStateException("Not comparable java field %s and field value %s"
                    .formatted(valueType, this)
            );
        }

        Preconditions.checkState(
                tuple.getType().equals(fieldType),
                "Tuple value cannot be converted to a composite of type %s: %s",
                fieldType,
                this
        );
        return tuple;
    }

    @Override
    public String toString() {
        return tuple.toString();
    }
}
