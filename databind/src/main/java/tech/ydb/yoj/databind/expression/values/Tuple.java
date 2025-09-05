package tech.ydb.yoj.databind.expression.values;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.schema.Schema;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static lombok.AccessLevel.NONE;
import static tech.ydb.yoj.databind.schema.Schema.PATH_DELIMITER;
import static tech.ydb.yoj.databind.schema.naming.NamingStrategy.NAME_DELIMITER;

@Value
public class Tuple implements Comparable<Tuple> {
    @Nullable
    @Getter(NONE)
    Object composite;

    @NonNull
    List<FieldAndValue> components;

    @NonNull
    @Getter(NONE)
    Schema.JavaField rootField;

    /**
     * <strong>This is a YOJ-internal API used by the YOJ implementation.</strong>
     * <p>Please do not construct a {@code Tuple} directly,
     * use {@link tech.ydb.yoj.databind.expression.values.FieldValue#getComparable(Map, Schema.JavaField)}
     * or {@link FieldValue#ofObj(Object, Schema.JavaField)} and get the underlying {@code Tuple} from the {@link TupleFieldValue}.
     *
     * @param composite raw composite field the components of which will make the tuple; {@code null} if not available
     * @param components tuple components (schema fields and their values in the tuple; values may be {@code null})
     * @param rootField schema field representing the tuple itself
     *
     * @see FieldAndValue
     * @see tech.ydb.yoj.databind.schema.Schema.JavaField
     */
    @InternalApi
    @java.beans.ConstructorProperties({"composite", "components", "rootField"})
    public Tuple(@Nullable Object composite, @NonNull List<FieldAndValue> components, @NonNull Schema.JavaField rootField) {
        this.composite = composite;
        this.components = components;
        this.rootField = rootField;
    }

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
    public int hashCode() {
        return 59 + compatFvListHashCode(components);
    }

    private int compatFvListHashCode(List<FieldAndValue> components) {
        int result = 1;
        for (FieldAndValue fv : components) {
            result = 31 * result + compatFvHashCode(fv);
        }
        return result;
    }

    private int compatFvHashCode(FieldAndValue fv) {
        int result = compatFieldHashCode(fv.field);
        result = 31 * result + (fv.value == null ? 0 : fv.value.hashCode());
        return result;
    }

    private int compatFieldListHashCode(List<Schema.JavaField> fields) {
        int result = 1;
        for (Schema.JavaField jf : fields) {
            result = 31 * result + compatFieldHashCode(jf);
        }
        return result;
    }

    private int compatFieldHashCode(Schema.JavaField field) {
        String selfName = field.getName().substring(rootField.getName().length() + NAME_DELIMITER.length());
        String selfPath = field.getPath().substring(rootField.getPath().length() + PATH_DELIMITER.length());

        int result = 1;
        result = 31 * result + field.getType().getTypeName().hashCode();
        result = 31 * result + field.getValueType().name().hashCode();
        result = 31 * result + selfName.hashCode();
        result = 31 * result + selfPath.hashCode();
        result = 31 * result + (field.isSimple() ? 0 : compatFieldListHashCode(field.getChildren()));

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Tuple other && components.equals(other.components);
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

        private Type fieldType() {
            return field.getType();
        }

        private String fieldPath() {
            return field.getPath();
        }

        @Override
        public int hashCode() {
            // NB: We keep the JDK 17 hashCode() implementation forever, for backwards compatibility with older versions of YOJ!
            int result = field.hashCode();
            result = 31 * result + (value == null ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof FieldAndValue other
                    && field.equals(other.field)
                    && Objects.equals(value, other.value);
        }
    }
}
