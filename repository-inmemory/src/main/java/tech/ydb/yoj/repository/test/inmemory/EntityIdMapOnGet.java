package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Entity;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class EntityIdMapOnGet<T extends Entity<T>, E> extends AbstractMap<Entity.Id<T>, E> {
    private final Comparator<Entity.Id<T>> comparator;
    private final Map<Entity.Id<T>, E> entries = new LinkedHashMap<>();
    private Set<Entry<Entity.Id<T>, E>> cachedEntrySet = null;

    public EntityIdMapOnGet(Comparator<Entity.Id<T>> comparator) {
        this.comparator = comparator;
    }

    @Override
    public Set<Entry<Entity.Id<T>, E>> entrySet() {
        if (cachedEntrySet == null) {
            List<Entry<Entity.Id<T>, E>> sorted = new ArrayList<>(entries.entrySet());
            sorted.sort(Comparator.comparing(Entry::getKey, comparator));
            cachedEntrySet = new LinkedHashSet<>(sorted);
        }
        return cachedEntrySet;
    }

    @Override
    public E get(Object key) {
        return entries.get(key);
    }

    @Override
    public E put(Entity.Id<T> key, E value) {
        E old = entries.put(key, value);
        cachedEntrySet = null; // invalidate cache
        return old;
    }

    @Override
    public E remove(Object key) {
        E removed = entries.remove(key);
        if (removed != null) {
            cachedEntrySet = null; // invalidate cache
        }
        return removed;
    }

    @Override
    public void clear() {
        entries.clear();
        cachedEntrySet = null;
    }

    @Override
    public boolean containsKey(Object key) {
        return entries.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return entries.containsValue(value);
    }

    @Override
    public int size() {
        return entries.size();
    }
}
