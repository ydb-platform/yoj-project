package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TreeMap<T, E> - is very expensive due to very complicated comparison login
 */
class EntityIdMap<T extends Entity<T>, E> extends AbstractMap<Entity.Id<T>, E> {
    private final Comparator<Entity.Id<T>> comparator;
    private Map<Entity.Id<T>, E> entries = new LinkedHashMap<>(); // probably not the best here but very simple

    public EntityIdMap(Comparator<Entity.Id<T>> comparator) {
        this.comparator = comparator;
    }

    public EntityIdMap(EntityIdMap<T, E> entityLines) {
        this(entityLines.comparator);
        entries = new LinkedHashMap<>(entityLines);
    }

    @Override
    public Set<Entry<Entity.Id<T>, E>> entrySet() {
        return entries.entrySet();
    }

    @Override
    public E get(Object key) {
        return entries.get(key);
    }

    @Override
    public E put(Entity.Id<T> key, E value) {
        List<Entity.Id<T>> temp = new ArrayList<>(entries.keySet());
        int index = Collections.binarySearch(temp, key, comparator);
        Map<Entity.Id<T>, E> result = new LinkedHashMap<>();
        E oldValue = null;
        int i = 0;
        for (Entry<Entity.Id<T>, E> entry : entries.entrySet()) {
            if (index < 0 && -index - 1 == i) {
                result.put(key, value);
                result.put(entry.getKey(), entry.getValue());
            } else if (index == i) {
                oldValue = entry.getValue();
                result.put(key, value);
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
            i++;
        }
        if (-index - 1 == result.size()) {
            result.put(key, value); // add last (item i.e first item or item with max value to existing list)
        }
        entries = result;
        return oldValue;
    }

    @Override
    public E remove(Object key) {
        return entries.remove(key);
    }
}
