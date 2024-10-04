package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static lombok.AccessLevel.NONE;

@Value
public class Tuple implements Comparable<Tuple> {
    @Nullable
    @Getter(NONE)
    @EqualsAndHashCode.Exclude
    Object composite;

    @NonNull
    List<FieldAndValue> components;

    @NonNull
    public Type getType() {
        Preconditions.checkArgument(composite != null, "this tuple has no corresponding composite object");
        return composite.getClass();
    }

    @NonNull
    public Object asComposite() {
        Preconditions.checkArgument(composite != null, "this tuple has no corresponding composite object");
        return composite;
    }

    @NonNull
    public Stream<FieldAndValue> streamComponents() {
        return components.stream();
    }

    @NonNull
    public String toString() {
        return components.stream().map(fv -> String.valueOf(fv.value())).collect(joining(", ", "<", ">"));
    }

    @Override
    public int compareTo(@NonNull Tuple other) {
        // sort shorter tuples first
        if (components.size() < other.components.size()) {
            return -1;
        }
        if (components.size() > other.components.size()) {
            return 1;
        }

        int i = 0;
        var thisIter = components.iterator();
        var otherIter = other.components.iterator();
        while (thisIter.hasNext()) {
            FieldAndValue thisComponent = thisIter.next();
            FieldAndValue otherComponent = otherIter.next();

            Comparable<?> thisValue = thisComponent.toComparable();
            Comparable<?> otherValue = otherComponent.toComparable();
            // sort null first
            if (thisValue == null && otherValue == null) {
                continue;
            }
            if (thisValue == null /* && otherValue != null */) {
                return -1;
            }
            if (otherValue == null /* && thisValue != null */) {
                return 1;
            }

            Preconditions.checkState(
                    thisComponent.fieldType().equals(otherComponent.fieldType()),
                    "Different tuple component types at [%s](%s): %s and %s",
                    i, thisComponent.fieldPath(), thisComponent.fieldType(), otherComponent.fieldType()
            );

            @SuppressWarnings({"rawtypes", "unchecked"})
            int res = ((Comparable) thisValue).compareTo(otherValue);
            if (res != 0) {
                return res;
            }

            i++;
        }
        return 0;
    }

    public record FieldAndValue(
            @NonNull Schema.JavaField field,
            @Nullable FieldValue value
    ) {
        public FieldAndValue(@NonNull Schema.JavaField jf, @NonNull Map<String, Object> flattenedObj) {
            this(jf, getValue(jf, flattenedObj));
        }

        public FieldAndValue {
            Preconditions.checkArgument(field.isFlat(), "field must be flat");
        }

        @Nullable
        private static FieldValue getValue(@NonNull Schema.JavaField jf, @NonNull Map<String, Object> flattenedObj) {
            String name = jf.getName();
            return flattenedObj.containsKey(name) ? FieldValue.ofObj(flattenedObj.get(name), jf) : null;
        }

        @Nullable
        public Comparable<?> toComparable() {
            return value == null ? null : value.getComparable(field);
        }

        public Type fieldType() {
            return field.getType();
        }

        public String fieldPath() {
            return field.getPath();
        }
    }
}
