package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/234")
public final class YqlTupleValue {
    private final List<String> fieldPaths;
    private final Map<String, Object> values;

    /*package*/ YqlTupleValue(@NonNull List<String> fieldPaths, @NonNull List</*@NonNull*/ ?> fieldValues) {
        Preconditions.checkArgument(!fieldPaths.isEmpty(), "Tuple field path list must not be empty");
        Preconditions.checkArgument(!fieldValues.isEmpty(), "Tuple field value list must not be empty");

        this.fieldPaths = new ArrayList<>();
        this.values = new LinkedHashMap<>();

        int index = 0;
        Iterator<String> fieldPathsIterator = fieldPaths.iterator();
        Iterator<?> fieldValuesIterator = fieldValues.iterator();
        while (fieldPathsIterator.hasNext() && fieldValuesIterator.hasNext()) {
            String fieldPath = fieldPathsIterator.next();
            Preconditions.checkArgument(fieldPath != null,
                    "Tuple field path (at index %s) must not be null", index);
            this.fieldPaths.add(fieldPath);

            Object fieldValue = fieldValuesIterator.next();
            Preconditions.checkArgument(fieldValue != null,
                    "Tuple value for field '%s' (at index %s) must not be null", fieldPath, index);

            Preconditions.checkArgument(!values.containsKey(fieldPath),
                    "Tuple field path (at index %s) must be unique, but got '%s' multiple times", fieldPath, index);
            values.put(fieldPath, fieldValue);

            index++;
        }
        Preconditions.checkArgument(!fieldPathsIterator.hasNext(),
                "Got more fields in a tuple than values (> %s)", index);
        Preconditions.checkArgument(!fieldValuesIterator.hasNext(),
                "Got more values in a tuple than fields (> %s)", index);
    }

    @NonNull
    public List<Schema.JavaField> toFieldList(Schema<?> schema) {
        return fieldPaths.stream().map(path -> schema.getField(path).toFlatField()).toList();
    }

    @NonNull
    public Object getValueOf(@NonNull Schema.JavaField field) {
        Object value = values.get(field.getPath());
        Preconditions.checkArgument(value != null, "No value for field '%s' in tuple", field, fieldPaths);
        return value;
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof YqlTupleValue other && this.values.equals(other.values);
    }

    @Override
    public String toString() {
        return "YqlTupleValue" + values;
    }
}
