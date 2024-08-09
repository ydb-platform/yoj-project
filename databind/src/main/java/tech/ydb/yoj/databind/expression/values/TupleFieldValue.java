package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import tech.ydb.yoj.databind.FieldValueType;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;

public record TupleFieldValue(
        Tuple tuple
) implements FieldValue {
    @Override
    public Optional<Comparable<?>> getComparableByType(Type fieldType, FieldValueType valueType) {
        if (Objects.requireNonNull(valueType) != FieldValueType.COMPOSITE) {
            return Optional.empty();
        }

        Preconditions.checkState(
                tuple.getType().equals(fieldType),
                "Tuple value cannot be converted to a composite of type %s: %s",
                fieldType,
                this
        );
        return Optional.of(tuple);
    }

    @Override
    public String toString() {
        return tuple.toString();
    }
}
