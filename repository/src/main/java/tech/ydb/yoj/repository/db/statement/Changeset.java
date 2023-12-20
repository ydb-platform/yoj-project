package tech.ydb.yoj.repository.db.statement;

import lombok.NonNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
 * have specific requirements.
 * <p>Blind updates disrupt query merging mechanism, so you typically won't able to run multiple blind update statements
 * in the same transaction, or interleave them with upserts ({@code Table.save()}) and inserts.
 * <p>Blind updates also do not update projections because they do not load the entity before performing the update;
 * this can cause projections to be inconsistent with the main entity.
 */
@Deprecated
public final class Changeset {
    private final Map<String, Object> newValues = new LinkedHashMap<>();

    public Changeset() {
    }

    public static <V> Changeset setField(String fieldPath, V value) {
        return new Changeset().set(fieldPath, value);
    }

    public <T> Changeset set(@NonNull String fieldPath, T value) {
        this.newValues.put(fieldPath, value);
        return this;
    }

    public Changeset setAll(@NonNull Changeset other) {
        return setAll(other.newValues);
    }

    public Changeset setAll(@NonNull Map<String, ?> fieldValues) {
        this.newValues.putAll(fieldValues);
        return this;
    }

    public Map<String, ?> toMap() {
        return new LinkedHashMap<>(this.newValues);
    }
}
