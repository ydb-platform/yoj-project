package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Range<ID extends Entity.Id<?>> {
    EntityIdSchema<ID> type;
    Map<String, Object> eqMap;
    Map<String, Object> minMap;
    Map<String, Object> maxMap;

    public static <ID extends Entity.Id<?>> Range<ID> create(@NonNull ID partial) {
        return create(partial, partial);
    }

    @SuppressWarnings({"unchecked"})
    public static <ID extends Entity.Id<?>> Range<ID> create(@NonNull ID min, @NonNull ID max) {
        Preconditions.checkArgument(min.getClass() == max.getClass(), "Min and max must be instances of the same class");
        EntityIdSchema<ID> type = (EntityIdSchema<ID>) EntityIdSchema.of(min.getClass());
        Map<String, Object> mn = type.flatten(min);
        Map<String, Object> mx = type.flatten(max);

        return create(type, mn, mx);
    }

    public static <ID extends Entity.Id<?>> Range<ID> create(EntityIdSchema<ID> type, Map<String, Object> map) {
        return create(type, map, map);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <ID extends Entity.Id<?>> Range<ID> create(
            EntityIdSchema<ID> type, Map<String, Object> mn, Map<String, Object> mx
    ) {
        Map<String, Object> eqMap = new HashMap<>();
        Map<String, Object> minMap = new HashMap<>();
        Map<String, Object> maxMap = new HashMap<>();

        StringBuilder s = new StringBuilder();
        for (String fn : type.flattenFieldNames()) {
            Comparable a = (Comparable) mn.get(fn);
            Comparable b = (Comparable) mx.get(fn);
            if (a == null && b == null) {
                s.append("0");
            } else if (a == null) {
                maxMap.put(fn, b);
                s.append("<");
            } else if (b == null) {
                minMap.put(fn, a);
                s.append("<");
            } else if (a.compareTo(b) < 0) {
                minMap.put(fn, a);
                maxMap.put(fn, b);
                s.append("<");
            } else if (a.compareTo(b) == 0) {
                s.append("=");
                eqMap.put(fn, a);
            } else {
                throw new IllegalArgumentException("min must be less or equal to max");
            }
        }
        Preconditions.checkArgument(s.toString().matches("=*<?0*"),
                "Fields of min and max must be filled in specific order: (equal)*(different)?(null)*");

        return new Range<>(type, eqMap, minMap, maxMap);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean contains(ID id) {
        Map<String, Object> flat = type.flatten(id);
        for (String fn : type.flattenFieldNames()) {
            Comparable c = (Comparable) flat.get(fn);
            if (c == null) {
                throw new IllegalArgumentException("Id fields cannot be null: " + id);
            }
            Object x = eqMap.get(fn);
            if (x != null && c.compareTo(x) != 0) {
                return false;
            }
            Object a = minMap.get(fn);
            if (a != null && c.compareTo(a) < 0) {
                return false;
            }
            Object b = maxMap.get(fn);
            if (b != null && c.compareTo(b) > 0) {
                return false;
            }
        }
        return true;
    }

    public List<Set<String>> getSchema() {
        return Stream.of(eqMap.keySet(), minMap.keySet(), maxMap.keySet())
                .collect(toList());
    }

    @Override
    public String toString() {
        ArrayList<String> list = new ArrayList<>();
        eqMap.forEach((k, v) -> list.add(k + "=" + v));
        minMap.forEach((k, v) -> list.add(k + ">=" + v));
        maxMap.forEach((k, v) -> list.add(k + "<=" + v));
        return "Range("
                + type.getType().getName().replaceFirst(".*\\.", "")
                + ": "
                + String.join(", ", list)
                + ")";
    }
}
