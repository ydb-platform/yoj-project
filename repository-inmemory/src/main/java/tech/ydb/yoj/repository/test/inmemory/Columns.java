package tech.ydb.yoj.repository.test.inmemory;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueTypes;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.DbTypeQualifier;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.exception.ConversionException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static org.eclipse.collections.impl.tuple.Tuples.pair;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
/*package*/ final class Columns {
    public static final Columns EMPTY = new Columns(Maps.immutable.empty());

    private final ImmutableMap<String, Object> map;

    public static <T extends Entity<T>> Columns fromEntity(EntitySchema<T> schema, T entity) {
        Map<String, Object> cells = schema.flatten(entity);
        List<Pair<String, Object>> newValues = new ArrayList<>();
        for (Schema.JavaField field : schema.flattenFields()) {
            String column = field.getName();
            Object value = serialize(field, cells.get(column));
            newValues.add(pair(column, value));
        }
        return new Columns(Maps.immutable.<String, Object>empty().newWithAllKeyValues(newValues));
    }

    public Columns patch(EntitySchema<?> schema, Map<String, Object> patch) {
        Set<String> columnNames = new HashSet<>(schema.flattenFieldNames());
        for (String column : patch.keySet()) {
            if (!columnNames.contains(column)) {
                String message = format("Invalid patch: unknown column name '%s'; does not belong to entity <%s>", column, schema.getTypeName());
                throw new ConversionException(message);
            }
        }

        List<Pair<String, Object>> newValues = new ArrayList<>();
        for (Schema.JavaField field : schema.flattenFields()) {
            String column = field.getName();
            if (patch.containsKey(column)) {
                Object value = serialize(field, patch.get(column));
                newValues.add(pair(column, value));
            }
        }
        return new Columns(map.newWithAllKeyValues(newValues));
    }

    public Map<String, Object> toMutableMap() {
        return this.map.toMap();
    }

    public <T> T toSchema(Schema<T> schema) {
        try {
            return toSchemaUnchecked(schema);
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            String message = format("Could not convert <%s>: %s", schema.getTypeName(), e.getMessage());
            throw new ConversionException(message, e);
        }
    }

    private <T> T toSchemaUnchecked(Schema<T> schema) {
        Map<String, Object> cells = new LinkedHashMap<>();
        for (Schema.JavaField field : schema.flattenFields()) {
            String column = field.getName();
            Object value = deserialize(field, this.map.get(column));
            cells.put(column, value);
        }
        return schema.newInstance(unmodifiableMap(cells));
    }

    private static Object serialize(Schema.JavaField field, Object value) {
        if (value == null) {
            return null;
        }
        Type type = field.getType();
        Type serializedType = field.getCustomValueTypeInfo() != null ? field.getCustomValueTypeInfo().getColumnClass() : type;
        String qualifier = field.getDbTypeQualifier();
        try {
            Preconditions.checkState(field.isSimple(), "Trying to serialize a non-simple field: %s", field);

            value = CustomValueTypes.preconvert(field, value);

            return switch (field.getValueType()) {
                case STRING, BOOLEAN, INTEGER, REAL -> value;
                // NB: YDB has native UUID support, but in-memory implementation always stores UUIDs as java.lang.String
                case UUID -> CommonConverters.serializeUuidValue(value);
                case ENUM -> DbTypeQualifier.ENUM_TO_STRING.equals(qualifier)
                        ? CommonConverters.serializeEnumToStringValue(serializedType, value)
                        : CommonConverters.serializeEnumValue(serializedType, value);
                case OBJECT -> CommonConverters.serializeOpaqueObjectValue(serializedType, value);
                case BINARY -> ((byte[]) value).clone();
                case BYTE_ARRAY -> ((ByteArray) value).copy().getArray();
                // TODO: Unify Instant and Duration handling in InMemory and YDB Repository
                case INTERVAL, TIMESTAMP -> value;
                default -> throw new IllegalStateException("Don't know how to serialize field: " + field);
            };
        } catch (Exception e) {
            throw new ConversionException("Could not serialize value of type <" + type + ">", e);
        }
    }

    private static Object deserialize(Schema.JavaField field, Object value) {
        if (value == null) {
            return null;
        }
        Type type = field.getType();
        Type serializedType = field.getCustomValueTypeInfo() != null ? field.getCustomValueTypeInfo().getColumnClass() : type;
        String qualifier = field.getDbTypeQualifier();
        try {
            Preconditions.checkState(field.isSimple(), "Trying to deserialize a non-simple field: %s", field);

            var deserialized = switch (field.getValueType()) {
                case STRING, BOOLEAN, INTEGER, REAL -> value;
                // NB: YDB has native UUID support, but in-memory implementation always stores UUIDs as java.lang.String
                case UUID -> CommonConverters.deserializeUuidValue(value);
                case ENUM -> DbTypeQualifier.ENUM_TO_STRING.equals(qualifier)
                        ? CommonConverters.deserializeEnumToStringValue(serializedType, value)
                        : CommonConverters.deserializeEnumValue(serializedType, value);
                case OBJECT -> CommonConverters.deserializeOpaqueObjectValue(serializedType, value);
                case BINARY -> ((byte[]) value).clone();
                case BYTE_ARRAY -> ByteArray.copy((byte[]) value);
                // TODO: Unify Instant and Duration handling in InMemory and YDB Repository
                case INTERVAL, TIMESTAMP -> value;
                default -> throw new IllegalStateException("Don't know how to deserialize field: " + field);
            };

            return deserialized == null ? null : CustomValueTypes.postconvert(field, deserialized);
        } catch (Exception e) {
            throw new ConversionException("Could not deserialize value of type <" + type + ">", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Columns columns = (Columns) o;
        return map.equals(columns.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }
}
