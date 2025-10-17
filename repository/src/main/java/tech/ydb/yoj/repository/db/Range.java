package tech.ydb.yoj.repository.db;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.util.lang.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a range of {@link Entity.Id Entity IDs}.
 * <ul>
 * <li>To create a range from entity IDs, use the {@code Range.create()} methods.</li>
 * <li>To create a range from entity IDs' column values, use the {@code EntityIdSchema.newRangeInstance()} methods.</li>
 * </ul>
 * <hr>
 *
 * <p>Note that the {@code Range} class has some serious limitations:
 * <ul>
 * <li>It's not possible to represent an open or a half-open interval of IDs (with one of ID bounds, or both, strictly
 * greater/less than the specified value). Use {@code Table.query()} instead for this purpose, <em>e.g.</em>:
 * {@code query().where("id.a").gt(...).and("id.a").lt(...).find()}.</li>
 * <li>{@code Range} cannot be disjoint: it always represents a monotonically increasing sequence of entity ID values.
 * Use {@code Table.find(Set<ID>)} to query for multiple ID prefixes (or multiple unordered exact IDs) at once.</li>
 * <li>Using {@code Range} for ID prefixes requires the entity ID class to have nullable fields, because
 * all prefix ID fields are required to have a non-{@code null} value, and all non-prefix ID fields are required
 * to be {@code null}. If the ID class absolutely cannot have nullable fields, use {@code Table.query()} instead,
 * <em>e.g.:</em> {@code query().where("id.prefixField1").eq(...).and("id.prefixField2").eq(...).find()}, though
 * this might be a bit less efficient, because the prefix values won't become a tuple in the YDB query.</li>
 * </ul>
 *
 * @param <ID> Entity ID type
 */
@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Range<ID extends Entity.Id<?>> {
    EntityIdSchema<ID> type;
    Map<JavaField, Object> eqMap;
    Map<JavaField, Object> minMap;
    Map<JavaField, Object> maxMap;

    /**
     * Creates a new {@link Range} representing all the IDs that have the same ID prefix specified by {@code partial}
     * (with "any ID field value" indicated by {@code null} value of a field in {@code partial} ID).
     * <br>A default entity ID schema provided by {@link EntityIdSchema#of(Class) EntityIdSchema.of(partial.getClass())}
     * is assumed.
     *
     * @param <ID>    Entity ID type
     * @param partial <em>Partial ID</em>, representing an ID prefix (with prefix fields being non-{@code null} and all
     *                other ID fields set to {@code null})
     * @return {@code Range} for the specified ID prefix
     * @throws IllegalArgumentException {@code partial} does not represent an ID prefix.
     *                                  <br><em>E.g.</em>, for a four-column ID {@code (a, b, c, d)}, a
     *                                  {@code partial} ID with only {@code a != null} and {@code c != null}
     *                                  will be invalid because {@code b} must also be {@code != null}.
     * @see Range#create(EntityIdSchema, Entity.Id)
     * @see Range#create(Entity.Id, Entity.Id)
     */
    public static <ID extends Entity.Id<?>> Range<ID> create(@NonNull ID partial) {
        return create(partial, partial);
    }

    /**
     * Creates a new {@link Range} representing all the IDs between the two specified IDs, inclusive.
     * <br>A default entity ID schema provided by {@link EntityIdSchema#of(Class)} is assumed.
     *
     * @param <ID>         Entity ID type
     * @param minInclusive min (inclusive) ID
     * @param maxInclusive max (inclusive) ID
     * @return {@code Range} between the specified minimum and maximum IDs, inclusive
     * @throws IllegalArgumentException The {@code minInclusive} ID is greater than the {@code maxInclusive} ID
     * @see Range#create(EntityIdSchema, Entity.Id, Entity.Id)
     * @see EntityIdSchema#newRangeInstance(Map, Map)
     */
    @SuppressWarnings("unchecked")
    public static <ID extends Entity.Id<?>> Range<ID> create(@NonNull ID minInclusive, @NonNull ID maxInclusive) {
        return create((EntityIdSchema<ID>) EntityIdSchema.of(minInclusive.getClass()), minInclusive, maxInclusive);
    }

    /**
     * Creates a new {@link Range} representing all the IDs that have the same ID prefix specified by {@code partial}
     * (with "any ID field value" indicated by {@code null} value of a field in {@code partial} ID).
     *
     * @param <ID>    Entity ID type
     * @param type    Entity ID schema to use
     * @param partial <em>Partial ID</em>, representing an ID prefix (with prefix fields being non-{@code null} and all
     *                other ID fields set to {@code null})
     * @return {@code Range} for the specified ID prefix
     * @throws IllegalArgumentException {@code partial} does not represent an ID prefix.
     *                                  <br><em>E.g.</em>, for a four-column ID {@code (a, b, c, d)}, a
     *                                  {@code partial} ID with only {@code a != null} and {@code c != null}
     *                                  will be invalid because {@code b} must also be {@code != null}.
     * @see Range#create(EntityIdSchema, Entity.Id, Entity.Id)
     * @see EntityIdSchema#newRangeInstance(Map)
     */
    public static <ID extends Entity.Id<?>> Range<ID> create(@NonNull EntityIdSchema<ID> type, @NonNull ID partial) {
        return internalCreate(type, type.flatten(partial));
    }

    /**
     * Creates a new {@link Range} representing all the IDs between the two specified IDs, inclusive.
     *
     * @param <ID>         Entity ID type
     * @param type         Entity ID schema to use
     * @param minInclusive min (inclusive) ID
     * @param maxInclusive max (inclusive) ID
     * @return {@code Range} between the specified minimum and maximum IDs, inclusive
     * @throws IllegalArgumentException The {@code minInclusive} ID is greater than the {@code maxInclusive} ID
     * @see EntityIdSchema#newRangeInstance(Map, Map)
     */
    public static <ID extends Entity.Id<?>> Range<ID> create(
            @NonNull EntityIdSchema<ID> type,
            @NonNull ID minInclusive,
            @NonNull ID maxInclusive
    ) {
        Preconditions.checkArgument(minInclusive.getClass().equals(maxInclusive.getClass()),
                "ID bounds must be instances of the same class, but got minInclusive: <%s> and maxInclusive: <%s>",
                minInclusive.getClass(), maxInclusive.getClass());
        return internalCreate(type, type.flatten(minInclusive), type.flatten(maxInclusive));
    }

    /*package*/
    static <ID extends Entity.Id<?>> Range<ID> internalCreate(
            @NonNull EntityIdSchema<ID> type,
            @NonNull Map<String, Object> idPrefix
    ) {
        return internalCreate(type, idPrefix, idPrefix);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    /*package*/ static <ID extends Entity.Id<?>> Range<ID> internalCreate(
            EntityIdSchema<ID> type,
            Map<String, Object> mn,
            Map<String, Object> mx
    ) {
        Map<JavaField, Object> eqMap = new LinkedHashMap<>();
        Map<JavaField, Object> minMap = new LinkedHashMap<>();
        Map<JavaField, Object> maxMap = new LinkedHashMap<>();

        StringBuilder s = new StringBuilder();
        for (JavaField jf : type.flattenFields()) {
            Comparable a = (Comparable) mn.get(jf.getName());
            Comparable b = (Comparable) mx.get(jf.getName());
            if (a == null && b == null) {
                s.append("0");
            } else if (a == null) {
                maxMap.put(jf, b);
                s.append("<");
            } else if (b == null) {
                minMap.put(jf, a);
                s.append("<");
            } else if (a.compareTo(b) < 0) {
                minMap.put(jf, a);
                maxMap.put(jf, b);
                s.append("<");
            } else if (a.compareTo(b) == 0) {
                s.append("=");
                eqMap.put(jf, a);
            } else {
                throw new IllegalArgumentException("min must be less or equal to max");
            }
        }
        Preconditions.checkArgument(s.toString().matches("=*<?0*"),
                "Fields of min and max must be filled in specific order: (equal)*(different)?(null)*");

        return new Range<>(type, eqMap, minMap, maxMap);
    }

    @InternalApi
    public Map<JavaField, Object> getEqMap() {
        return eqMap;
    }

    @InternalApi
    public Map<JavaField, Object> getMaxMap() {
        return maxMap;
    }

    @InternalApi
    public Map<JavaField, Object> getMinMap() {
        return minMap;
    }

    /**
     * Checks whether the specified ID is contained within this {@code Range}.
     *
     * @param id ID to check. <strong>Must not</strong> be <em>partial</em> (with ID prefix fields set
     *          to non-{@code null} values and all other ID fields set to {@code null})
     * @return {@code true} if the specified {@code id} is in Range; {@code false} otherwise
     * @throws IllegalArgumentException {@code id} is partial
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean contains(@NonNull ID id) {
        Map<String, Object> flat = type.flatten(id);
        for (JavaField jf : type.flattenFields()) {
            Comparable c = (Comparable) flat.get(jf.getName());
            Preconditions.checkArgument(c != null, "ID fields cannot be null, but got %s=null in %s", jf.getPath(), id);

            Object x = eqMap.get(jf);
            if (x != null && c.compareTo(x) != 0) {
                return false;
            }
            Object a = minMap.get(jf);
            if (a != null && c.compareTo(a) < 0) {
                return false;
            }
            Object b = maxMap.get(jf);
            if (b != null && c.compareTo(b) > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        List<String> list = new ArrayList<>();
        eqMap.forEach((k, v) -> list.add(k.getPath() + " == " + v));
        minMap.forEach((k, v) -> list.add(k.getPath() + " >= " + v));
        maxMap.forEach((k, v) -> list.add(k.getPath() + " <= " + v));

        String entityTypeName = Types.getShortTypeName(type.getEntityType());
        return "Range[" + entityTypeName + "]("
                + (list.isEmpty() ? "*" : String.join(" && ", list)) + ")";
    }
}
