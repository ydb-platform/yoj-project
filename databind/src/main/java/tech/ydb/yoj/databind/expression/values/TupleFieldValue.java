package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.Schema;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;

public record TupleFieldValue(@NonNull Tuple tuple) implements FieldValue {
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
    public Object getRaw(@NonNull Schema.JavaField field) {
        field = field.isFlat() ? field.toFlatField() : field;

        if (FieldValueType.forSchemaField(field).isComposite()) {
            Type fieldType = field.getType();
            Preconditions.checkState(tuple.getType().equals(fieldType),
                    "Tuple value cannot be converted to a composite of type %s: %s", fieldType, this);
            // Composite values are never postconvert()ed so we don't need to reuse FieldValue.super.getRaw() logic here
            return tuple.asComposite();
        }

        return FieldValue.super.getRaw(field);
    }

    @Override
    public String toString() {
        return tuple.toString();
    }
}
