package tech.ydb.yoj.repository.test.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Zone {
    // This class provides singletones. Available to use ==
    private static final Map<String, Zone> singletones = new HashMap<>();

    public static final Zone ZONE_A = Zone.of("zone-a");
    public static final Zone ZONE_B = Zone.of("zone-b");
    public static final Zone ZONE_C = Zone.of("zone-c");
    public static final Zone ZONE_E = Zone.of("zone-e");
    public static final Zone ZONE_D = Zone.of("zone-d");

    private final String value;

    @JsonCreator
    public static Zone of(String newValue) {
        String value = newValue.toLowerCase();
        if (!singletones.containsKey(value)) {
            singletones.put(value, new Zone(value, true));
        }
        return singletones.get(value);
    }

    // for string value deserialization
    public static Zone fromString(String stringValue) {
        return Zone.of(stringValue);
    }

    @JsonIgnore
    private Zone(String value, boolean ignored) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    public boolean in(Zone... envs) {
        return List.of(envs).stream().anyMatch(s -> s.equals(this));
    }
}
