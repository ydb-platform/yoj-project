package tech.ydb.yoj.repository.test.inmemory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityExpressions;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;
import tech.ydb.yoj.repository.db.exception.IllegalTransactionIsolationLevelException;
import tech.ydb.yoj.repository.db.list.InMemoryQueries;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class InMemoryTable<T extends Entity<T>> implements Table<T> {
    private final EntitySchema<T> schema;
    private final TableDescriptor<T> tableDescriptor;
    private final InMemoryRepositoryTransaction transaction;

    @Deprecated // Don't use DbMemory, use other constructor instead
    public InMemoryTable(DbMemory<T> memory) {
        this(memory.transaction(), memory.type());
    }

    public InMemoryTable(InMemoryRepositoryTransaction transaction, Class<T> type) {
        this(transaction, TableDescriptor.from(EntitySchema.of(type)));
    }

    public InMemoryTable(InMemoryRepositoryTransaction transaction, TableDescriptor<T> tableDescriptor) {
        this.schema = EntitySchema.of(tableDescriptor.entityType());
        this.tableDescriptor = tableDescriptor;
        this.transaction = transaction;
    }

    @Override
    public List<T> findAll() {
        transaction.getWatcher().markTableRead(tableDescriptor);
        return findAll0();
    }

    @Override
    public <V extends View> List<V> findAll(Class<V> viewType) {
        return findAll().stream()
                .map(entity -> toView(viewType, schema, entity))
                .collect(toList());
    }

    @Override
    public long countAll() {
        return findAll().size();
    }

    @Override
    public long count(String indexName, FilterExpression<T> filter) {
        return find(indexName, filter, null, null, null).size();
    }

    @Override
    @Deprecated
    public void update(Entity.Id<T> id, Changeset changeset) {
        T found = find(id);
        if (found == null) {
            return;
        }
        Map<String, Object> cells = new HashMap<>(schema.flatten(found));

        changeset.toMap().forEach((k, v) -> {
            cells.putAll(schema.flattenOneField(k, v));
        });

        T newInstance = schema.newInstance(cells);

        save(newInstance);
    }

    @Override
    public List<T> find(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset
    ) {
        // NOTE: InMemoryTable doesn't handle index.
        return InMemoryQueries.find(() -> findAll().stream(), filter, orderBy, limit, offset);
    }

    @Override
    public <V extends View> List<V> find(
            Class<V> viewType,
            @Nullable String indexName,
            @Nullable FilterExpression<T> finalFilter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset,
            boolean distinct
    ) {
        Stream<V> stream = find(indexName, finalFilter, orderBy, limit, offset).stream()
                .map(entity -> toView(viewType, schema, entity));

        if (distinct) {
            stream = stream.distinct();
        }

        return stream.collect(toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ID extends Entity.Id<T>> List<ID> findIds(
            @Nullable String indexName,
            @Nullable FilterExpression<T> finalFilter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset
    ) {
        return find(indexName, finalFilter, orderBy, limit, offset).stream()
                .map(entity -> (ID) entity.getId())
                .collect(toList());
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params) {
        return readTableStream(params).map(T::postLoad);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params) {
        return readTableStream(params).map(e -> {
            @SuppressWarnings("unchecked")
            ID id = (ID) e.getId();
            return id;
        });
    }

    @Override
    public <V extends ViewId<T>, ID extends Entity.Id<T>> Stream<V> readTable(
            Class<V> viewClass, ReadTableParams<ID> params
    ) {
        return readTableStream(params).map(e -> toView(viewClass, schema, e));
    }

    @Override
    public Class<T> getType() {
        return tableDescriptor.entityType();
    }

    @Override
    public T find(Entity.Id<T> id) {
        if (id.isPartial()) {
            throw new IllegalArgumentException("Cannot use partial id in find method");
        }
        return transaction.getTransactionLocal().firstLevelCache().get(id, __ -> {
            markKeyRead(id);
            T entity = transaction.doInTransaction("find(" + id + ")", tableDescriptor, shard -> shard.find(id));
            return postLoad(entity);
        });
    }

    @Override
    public <V extends View> V find(Class<V> viewType, Entity.Id<T> id) {
        if (id.isPartial()) {
            throw new IllegalArgumentException("Cannot use partial id in find method");
        }

        FirstLevelCache cache = transaction.getTransactionLocal().firstLevelCache();
        if (cache.containsKey(id)) {
            return cache.peek(id)
                    .map(entity -> toView(viewType, schema, entity))
                    .orElse(null);
        }

        markKeyRead(id);
        return transaction.doInTransaction("find(" + id + ")", tableDescriptor, shard -> shard.find(id, viewType));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ID extends Entity.Id<T>> List<T> find(Range<ID> range) {
        transaction.getWatcher().markRangeRead(tableDescriptor, range);
        return findAll0().stream()
                .filter(e -> range.contains((ID) e.getId()))
                .sorted(EntityIdSchema.SORT_ENTITY_BY_ID)
                .collect(toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range) {
        return find(range).stream()
                .map(e -> (ID) e.getId())
                .collect(toList());
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range) {
        return find(range).stream()
                .map(entity -> toView(viewType, schema, entity))
                .collect(toList());
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids) {
        return find(viewType, ids, null, EntityExpressions.defaultOrder(getType()), null);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(
            Class<V> viewType,
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }

        return find(ids, filter, orderBy, limit).stream()
                .map(entity -> toView(viewType, schema, entity))
                .collect(toList());
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        var found = findUncached(ids, filter, orderBy, limit);
        return postLoad(found);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> findUncached(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }

        EntityIdSchema<Entity.Id<T>> idSchema = schema.getIdSchema();
        Set<Map<String, Object>> idsSet = ids.stream().map(idSchema::flatten).collect(toUnmodifiableSet());
        Set<Set<String>> idFieldsSet = idsSet.stream().map(Map::keySet).collect(toUnmodifiableSet());

        Preconditions.checkArgument(idFieldsSet.size() > 0, "ids must have at least one non-null field");
        Preconditions.checkArgument(idFieldsSet.size() == 1, "ids must have nulls in the same fields");

        Set<String> idFields = Iterables.getOnlyElement(idFieldsSet);

        ids.forEach(this::markKeyRead);

        Stream<T> result = getAllEntries().stream()
                .filter(e -> idsSet.contains(
                        idSchema.flatten(e.getId()).entrySet().stream()
                                .filter(entry -> idFields.contains(entry.getKey()))
                                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
                ));
        if (filter != null) {
            result = result.filter(InMemoryQueries.toPredicate(filter));
        }
        if (orderBy != null) {
            result = result.sorted(InMemoryQueries.toComparator(orderBy));
        }
        if (limit != null) {
            result = result.limit(limit);
        }

        return result.toList();
    }

    @Override
    public <V extends View, KEY> List<V> find(
            Class<V> viewType,
            String indexName,
            Set<KEY> keys,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (keys.isEmpty()) {
            return List.of();
        }

        return find(indexName, keys, filter, orderBy, limit).stream()
                .map(entity -> toView(viewType, schema, entity))
                .toList();
    }

    @Override
    public <KEY> List<T> find(
            String indexName,
            Set<KEY> keys,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (keys.isEmpty()) {
            return List.of();
        }

        @SuppressWarnings("unchecked")
        Class<KEY> keyType = (Class<KEY>) Iterables.getFirst(keys, null).getClass();

        Schema<KEY> keySchema = ObjectSchema.of(keyType);

        Set<Map<String, Object>> keysSet = keys.stream().map(keySchema::flatten).collect(toUnmodifiableSet());
        Set<Set<String>> keyFieldsSet = keysSet.stream().map(Map::keySet).collect(toUnmodifiableSet());

        Preconditions.checkArgument(keyFieldsSet.size() != 0, "keys should have at least one non-null field");
        Preconditions.checkArgument(keyFieldsSet.size() == 1, "keys should have nulls in the same fields");

        Set<String> keyFields = Iterables.getOnlyElement(keyFieldsSet);

        Schema.Index globalIndex = schema.getGlobalIndexes().stream()
                .filter(i -> i.getIndexName().equals(indexName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Table `%s` doesn't have index `%s`".formatted(tableDescriptor.toDebugString(), indexName)
                ));

        Set<String> indexKeys = Set.copyOf(globalIndex.getFieldNames());
        Set<String> missingInIndexKeys = Sets.difference(keyFields, indexKeys);

        Preconditions.checkArgument(
                missingInIndexKeys.isEmpty(),
                "Index `%s` of table `%s` doesn't contain key(s): [%s]".formatted(
                        indexName, tableDescriptor.toDebugString(), String.join(", ", missingInIndexKeys)
                )
        );
        Preconditions.checkArgument(
                isPrefixedFields(globalIndex.getFieldNames(), keyFields),
                "FindIn(keys) is allowed only by the prefix of the index key fields, index key: %s, query uses the fields: %s"
                        .formatted(globalIndex.getFieldNames(), keyFields)
        );

        for (Map<String, Object> id : keysSet) {
            transaction.getWatcher().markRangeRead(tableDescriptor, id);
        }

        Stream<T> result = getAllEntries().stream()
                .filter(e -> keysSet.contains(
                        schema.flatten(e).entrySet().stream()
                                .filter(field -> keyFields.contains(field.getKey()))
                                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
                ));
        if (filter != null) {
            result = result.filter(InMemoryQueries.toPredicate(filter));
        }
        if (orderBy != null) {
            result = result.sorted(InMemoryQueries.toComparator(orderBy));
        }
        if (limit != null) {
            result = result.limit(limit);
        }

        return postLoad(result.toList());
    }

    private boolean isPrefixedFields(List<String> keyFields, Set<String> fields) {
        for (var keyField : keyFields.subList(0, fields.size())) {
            if (!fields.contains(keyField)) {
                return false;
            }
        }

        return true;
    }

    private <ID extends Entity.Id<T>> void markKeyRead(ID id) {
        EntityIdSchema<Entity.Id<T>> idSchema = schema.getIdSchema();
        if (idSchema.flattenFieldNames().size() != idSchema.flatten(id).size()) {
            // Partial key, will throw error when not searching by PK prefix
            transaction.getWatcher().markRangeRead(tableDescriptor, Range.create(id, id));
        } else {
            transaction.getWatcher().markRowRead(tableDescriptor, id);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ID extends Entity.Id<T>> List<ID> findIds(Set<ID> ids) {
        return find(ids).stream()
                .map(e -> (ID) e.getId())
                .sorted(schema.getIdSchema())
                .toList();
    }

    @Override
    public T insert(T tt) {
        T t = tt.preSave();
        transaction.getWatcher().markRowRead(tableDescriptor, t.getId());
        transaction.doInWriteTransaction("insert(" + t + ")", tableDescriptor, shard -> shard.insert(t));
        transaction.getTransactionLocal().firstLevelCache().put(t);
        transaction.getTransactionLocal().projectionCache().save(t);
        return t;
    }

    @Override
    public T save(T tt) {
        T t = tt.preSave();
        transaction.doInWriteTransaction("save(" + t + ")", tableDescriptor, shard -> shard.save(t));
        transaction.getTransactionLocal().firstLevelCache().put(t);
        transaction.getTransactionLocal().projectionCache().save(t);
        return t;
    }

    @Override
    public void delete(Entity.Id<T> id) {
        transaction.doInWriteTransaction("delete(" + id + ")", tableDescriptor, shard -> shard.delete(id));
        transaction.getTransactionLocal().firstLevelCache().putEmpty(id);
        transaction.getTransactionLocal().projectionCache().delete(id);
    }

    @Override
    public void deleteAll() {
        transaction.doInWriteTransaction(
                "deleteAll(" + tableDescriptor.toDebugString() + ")", tableDescriptor, WriteTxDataShard::deleteAll
        );
    }

    private List<T> getAllEntries() {
        return transaction.doInTransaction(
                "findAll(" + tableDescriptor.toDebugString() + ")", tableDescriptor, ReadOnlyTxDataShard::findAll
        );
    }

    private List<T> findAll0() {
        List<T> all = getAllEntries();
        return postLoad(all);
    }

    @Override
    public Stream<T> streamAll(int batchSize) {
        return streamPartial(null, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize) {
        Preconditions.checkArgument(1 <= batchSize && batchSize <= 5000,
                "batchSize must be in range [1, 5000], got %s", batchSize);

        Range<ID> range = partial == null ? null : Range.create(partial);
        markRangeRead(range);

        return streamPartial0(range);
    }

    private <ID extends Entity.Id<T>> Stream<T> streamPartial0(@Nullable Range<ID> range) {
        return (range == null ? findAll() : find(range)).stream();
    }

    @Override
    public <V extends ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize) {
        return streamPartial(viewType, null, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>, V extends ViewId<T>> Stream<V> streamPartial(
            Class<V> viewType, ID partial, int batchSize
    ) {
        return streamPartial(partial, batchSize).map(e -> toView(viewType, schema, e));
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize) {
        return streamPartialIds(null, batchSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize) {
        Preconditions.checkArgument(1 <= batchSize && batchSize <= 10000,
                "batchSize must be in range [1, 10000], got %s", batchSize);

        Range<ID> range = partial == null ? null : Range.create(partial);
        markRangeRead(range);

        return streamPartial0(range).map(e -> (ID) e.getId());
    }

    private <ID extends Entity.Id<T>> void markRangeRead(Range<ID> range) {
        if (range == null) {
            transaction.getWatcher().markTableRead(tableDescriptor);
        } else {
            transaction.getWatcher().markRangeRead(tableDescriptor, range);
        }
    }

    private <ID extends Entity.Id<T>> Stream<T> readTableStream(ReadTableParams<ID> params) {
        if (!transaction.getOptions().getIsolationLevel().isReadOnly()) {
            throw new IllegalTransactionIsolationLevelException("readTable", transaction.getOptions().getIsolationLevel());
        }
        if (!params.isOrdered() && (params.getFromKey() != null || params.getToKey() != null)) {
            throw new IllegalArgumentException("using fromKey or toKey with unordered readTable does not make sense");
        }
        Stream<T> stream = findAll0()
                .stream()
                .filter(e -> readTableFilter(e, params));
        if (params.isOrdered()) {
            stream = stream.sorted(EntityIdSchema.SORT_ENTITY_BY_ID);
        }
        if (params.getRowLimit() > 0) {
            stream = stream.limit(params.getRowLimit());
        }
        return stream;
    }

    private <ID extends Entity.Id<T>> boolean readTableFilter(T e, ReadTableParams<ID> params) {
        @SuppressWarnings("unchecked")
        ID id = (ID) e.getId();
        ID from = params.getFromKey();
        if (from != null) {
            int compare = EntityIdSchema.ofEntity(id.getType()).compare(id, from);
            if (params.isFromInclusive() ? compare < 0 : compare <= 0) {
                return false;
            }
        }
        ID to = params.getToKey();
        if (to != null) {
            int compare = EntityIdSchema.ofEntity(id.getType()).compare(id, to);
            return params.isToInclusive() ? compare <= 0 : compare < 0;
        }
        return true;
    }

    @Override
    public FirstLevelCache getFirstLevelCache() {
        return transaction.getTransactionLocal().firstLevelCache();
    }

    @Nullable
    @Override
    public T postLoad(T entity) {
        if (entity == null) {
            return null;
        }
        T t = entity.postLoad();
        transaction.getTransactionLocal().firstLevelCache().put(t);
        transaction.getTransactionLocal().projectionCache().load(t);
        return t;
    }

    private static <V extends Table.View, T extends Entity<T>> V toView(
            Class<V> viewType, EntitySchema<T> schema, T entity
    ) {
        if (entity == null) {
            return null;
        }

        ViewSchema<V> viewSchema = ViewSchema.of(viewType);
        return Columns.fromEntity(schema, entity).toSchema(viewSchema);
    }


    @Deprecated // Legacy. Using only for creating InMemoryTable. Use constructor of InMemoryTable instead
    public record DbMemory<T extends Entity<T>>(
            Class<T> type,
            InMemoryRepositoryTransaction transaction
    ) {
    }
}
