package tech.ydb.yoj.repository.ydb.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Entity.Id;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;
import tech.ydb.yoj.repository.ydb.bulk.BulkMapper;
import tech.ydb.yoj.repository.ydb.bulk.BulkMapperImpl;
import tech.ydb.yoj.repository.ydb.readtable.EntityIdKeyMapper;
import tech.ydb.yoj.repository.ydb.readtable.ReadTableMapper;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.repository.ydb.statement.UpdateInStatement;
import tech.ydb.yoj.repository.ydb.statement.UpdateModel;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;
import tech.ydb.yoj.repository.ydb.yql.YqlLimit;
import tech.ydb.yoj.repository.ydb.yql.YqlListingQuery;
import tech.ydb.yoj.repository.ydb.yql.YqlOrderBy;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;
import tech.ydb.yoj.repository.ydb.yql.YqlView;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static tech.ydb.yoj.repository.db.EntityExpressions.defaultOrder;

public class YdbTable<T extends Entity<T>> implements Table<T> {
    private final QueryExecutor executor;
    @Getter
    private final Class<T> type;

    public YdbTable(Class<T> type, QueryExecutor executor) {
        this.type = type;
        this.executor = new CheckingQueryExecutor(executor);
    }

    protected YdbTable(QueryExecutor executor) {
        this.executor = new CheckingQueryExecutor(executor);
        this.type = resolveEntityType();
    }

    @SuppressWarnings("unchecked")
    private Class<T> resolveEntityType() {
        return (Class<T>) (new TypeToken<T>(getClass()) {
        }).getRawType();
    }

    @SafeVarargs
    private static <E> List<E> toList(E first, E... rest) {
        return concat(Stream.of(first), stream(rest)).collect(Collectors.toList());
    }

    @Override
    public List<T> findAll() {
        return postLoad(executor.execute(YqlStatement.findAll(type), null));
    }

    /**
     * Provides stream for all entities in a collection.
     * Makes mutliple queries if necessary, each selecting at most batchSize.
     * batchSize should be small enough to ensure that
     * batch does not exceed 40Mib
     *
     * @param batchSize number of entities to fetch in query to db. Max 5000.
     * @return stream of all entities in a collection
     */
    @Override
    public Stream<T> streamAll(int batchSize) {
        return streamPartial(null, batchSize);
    }

    /**
     * view support for {@link #streamAll(int)}
     */
    @Override
    public <V extends ViewId<T>> Stream<V> streamAll(Class<V> viewType, int batchSize) {
        return streamPartial(viewType, null, batchSize);
    }

    /**
     * Provides stream for entities in a collection filtered by partial PK.
     * Makes mutliple queries if necessary, each selecting at most batchSize.
     * batchSize should be small enough to ensure that
     * batch does not exceed 40Mib
     *
     * @param partial   partial PK
     * @param batchSize number of entities to fetch in query to db. Max 5000.
     * @return stream of selected entities in a collection
     */
    @Override
    public <ID extends Entity.Id<T>> Stream<T> streamPartial(ID partial, int batchSize) {
        return streamPartial(partial, batchSize, Entity::getId, YdbTable.this::find);
    }

    /**
     * view support for {@link #streamPartial(Entity.Id, int)}
     */
    @Override
    public <ID extends Entity.Id<T>, V extends ViewId<T>> Stream<V> streamPartial(Class<V> viewType, ID partial, int batchSize) {
        return streamPartial(partial, batchSize, ViewId::getId, (part, parts) -> YdbTable.this.find(viewType, part, parts));
    }

    private <R> Stream<R> streamPartial(
            Entity.Id<T> partial, int batchSize,
            Function<R, Entity.Id<T>> idMapper,
            BiFunction<YqlStatementPart<?>, YqlStatementPart<?>[], List<R>> findMethod
    ) {
        Preconditions.checkArgument(1 <= batchSize && batchSize <= 5000, "batchSize must be in range [1, 5000], got %s", batchSize);
        return StreamSupport.stream(new BatchFindSpliterator<>(type, partial, batchSize) {
            @Override
            protected Entity.Id<T> getId(R r) {
                return idMapper.apply(r);
            }

            @Override
            protected List<R> find(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
                return findMethod.apply(part, otherParts);
            }
        }, false);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> streamAllIds(int batchSize) {
        return streamPartialIds(null, batchSize);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> streamPartialIds(ID partial, int batchSize) {
        Preconditions.checkArgument(1 <= batchSize && batchSize <= 10000, "batchSize must be in range [1, 10000], got %s", batchSize);
        return StreamSupport.stream(new BatchFindSpliterator<>(type, partial, batchSize) {
            @Override
            protected ID getId(ID id) {
                return id;
            }

            @Override
            protected List<ID> find(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
                return findIds(part, otherParts);
            }
        }, false);
    }

    @Override
    public <V extends View> List<V> findAll(Class<V> viewType) {
        return executor.execute(YqlStatement.findAll(type, viewType), null);
    }

    @Override
    public void deleteAll() {
        executor.pendingExecute(YqlStatement.deleteAll(type), null);
    }

    @Override
    public void bulkUpsert(List<T> input, BulkParams params) {
        var entitySchema = EntitySchema.of(getType());
        var mapper = new BulkMapperImpl<>(entitySchema);
        executor.bulkUpsert(mapper, input, params);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params) {
        EntitySchema<T> entitySchema = EntitySchema.of(getType());
        ReadTableMapper<ID, T> mapper = new EntityIdKeyMapper<>(entitySchema, entitySchema);
        return readTableStream(mapper, params)
                .map(T::postLoad);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params) {
        EntitySchema<T> entitySchema = EntitySchema.of(getType());
        EntityIdSchema<ID> idSchema = entitySchema.getIdSchema();
        ReadTableMapper<ID, ID> mapper = new EntityIdKeyMapper<>(entitySchema, idSchema);
        return readTableStream(mapper, params);
    }

    @Override
    public <V extends ViewId<T>, ID extends Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params) {
        EntitySchema<T> entitySchema = EntitySchema.of(getType());
        ViewSchema<V> viewSchema = ViewSchema.of(viewClass);
        ReadTableMapper<ID, V> mapper = new EntityIdKeyMapper<>(entitySchema, viewSchema);
        return readTableStream(mapper, params);
    }

    private <K, V> Stream<V> readTableStream(ReadTableMapper<K, V> mapper, ReadTableParams<K> params) {
        if (!params.isOrdered() && (params.getFromKey() != null || params.getToKey() != null)) {
            throw new IllegalArgumentException("using fromKey or toKey with unordered readTable does not make sense");
        }

        return executor.readTable(mapper, params);
    }

    @Override
    public T find(Entity.Id<T> id) {
        if (id.isPartial()) {
            throw new IllegalArgumentException("Cannot use partial id in find method");
        }
        return executor.getTransactionLocal().firstLevelCache().get(id, __ -> {
            List<T> res = postLoad(executor.execute(YqlStatement.find(type), id));
            return res.isEmpty() ? null : res.get(0);
        });
    }

    @Override
    public <V extends View> V find(Class<V> viewType, Entity.Id<T> id) {
        List<V> res = executor.execute(YqlStatement.find(type, viewType), id);
        return res.isEmpty() ? null : res.get(0);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Range<ID> range) {
        return postLoad(executor.execute(YqlStatement.findRange(type, range), range));
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range) {
        return executor.execute(YqlStatement.findRange(type, viewType, range), range);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids) {
        return find(viewType, ids, null, defaultOrder(type), null);
    }

    public final List<T> find(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return find(toList(part, otherParts));
    }

    public List<T> find(Collection<? extends YqlStatementPart<?>> parts) {
        return postLoad(executor.execute(YqlStatement.find(type, parts), parts));
    }

    @Override
    public long countAll() {
        return count();
    }

    @Override
    public long count(String indexName, FilterExpression<T> filter) {
        YqlPredicate yqlFilter = filter == null ? null : YqlListingQuery.toYqlPredicate(filter);
        YqlView yqlView = indexName == null ? null : YqlView.index(indexName);

        var statementParts = Stream.of(yqlView, yqlFilter)
                .filter(Objects::nonNull)
                .toArray(YqlStatementPart[]::new);

        return count(statementParts);
    }

    @Override
    public List<T> find(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return find(statements);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(@Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return findIds(statements);
    }

    @Override
    public <V extends View> List<V> find(
            Class<V> viewType,
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter,
            @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit,
            @Nullable Long offset,
            boolean distinct
    ) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return find(viewType, statements, distinct);
    }

    @Override
    public <ID extends Id<T>> List<T> find(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        if (ids.isEmpty()) {
            return List.of();
        }
        var isPartialIdMode = ids.iterator().next().isPartial();
        List<T> found = postLoad(findUncached(ids, filter, orderBy, limit));
        if (!isPartialIdMode && ids.size() > found.size()) {
            Set<Id<T>> foundIds = found.stream().map(Entity::getId).collect(toSet());
            Sets.difference(ids, foundIds).forEach(executor.getTransactionLocal().firstLevelCache()::putEmpty);
        }
        return found;
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> findUncached(Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return executor.execute(YqlStatement.findIn(type, ids, filter, orderBy, limit), ids);
    }

    @Override
    public <V extends View, ID extends Id<T>> List<V> find(Class<V> viewType, Set<ID> ids, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return executor.execute(YqlStatement.findIn(type, viewType, ids, filter, orderBy, limit), ids);
    }

    @Override
    public <K> List<T> find(String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        if (keys.isEmpty()) {
            return List.of();
        }
        return postLoad(executor.execute(YqlStatement.findIn(type, indexName, keys, filter, orderBy, limit), keys));
    }

    @Override
    public <V extends View, K> List<V> find(Class<V> viewType, String indexName, Set<K> keys, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit) {
        if (keys.isEmpty()) {
            return List.of();
        }
        return executor.execute(YqlStatement.findIn(type, viewType, indexName, keys, filter, orderBy, limit), keys);
    }

    public static <T extends Entity<T>> List<YqlStatementPart<? extends YqlStatementPart<?>>> buildStatementParts(
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        return buildStatementParts(null, filter, orderBy, limit, offset);
    }

    public static <T extends Entity<T>> List<YqlStatementPart<? extends YqlStatementPart<?>>> buildStatementParts(
            @Nullable String indexName, @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy, @Nullable Integer limit, @Nullable Long offset) {
        Optional<Integer> limitO = Optional.ofNullable(limit);
        Optional<Long> offsetO = Optional.ofNullable(offset);

        YqlPredicate yqlFilter = filter == null ? null : YqlListingQuery.toYqlPredicate(filter);
        YqlOrderBy yqlOrderBy = orderBy == null ? null : YqlListingQuery.toYqlOrderBy(orderBy);

        YqlLimit yqlLimit = null;
        if (offsetO.orElse(0L) != 0L || limit != null) {
            yqlLimit = YqlLimit.range(offsetO.orElse(0L), offsetO.orElse(0L)
                    + limitO.orElseThrow(() -> new IllegalArgumentException("offset > 0 with limit=null is not supported")));
        }

        YqlView yqlView = indexName == null ? null : YqlView.index(indexName);

        return Stream.of(yqlFilter, yqlView, yqlOrderBy, yqlLimit)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public long count(YqlStatementPart<?>... parts) {
        List<YqlStatementPart<?>> partsList = asList(parts);
        return executor.execute(YqlStatement.count(type, partsList), partsList).get(0).getCount();
    }

    public <V extends View> List<V> find(Class<V> viewType, YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return find(viewType, toList(part, otherParts), false);
    }

    public <V extends View> List<V> find(Class<V> viewType, Collection<? extends YqlStatementPart<?>> parts, boolean distinct) {
        return executor.execute(YqlStatement.find(type, viewType, distinct, parts), parts);
    }

    public <ID extends Entity.Id<T>> List<ID> findIds(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return findIds(toList(part, otherParts));
    }

    private <ID extends Entity.Id<T>> List<ID> findIds(Collection<? extends YqlStatementPart<?>> parts) {
        return executor.execute(YqlStatement.findIds(type, parts), parts);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range) {
        return executor.execute(YqlStatement.findIds(type, range), range);
    }

    @Override
    public <ID extends Id<T>> List<ID> findIds(Set<ID> partialIds) {
        if (partialIds.isEmpty()) {
            return List.of();
        }
        return executor.execute(YqlStatement.findIdsIn(type, partialIds, null, defaultOrder(type), null), partialIds);
    }

    @Override
    public void update(Entity.Id<T> id, Changeset changeset) {
        UpdateModel.ById<Id<T>> model = new UpdateModel.ById<>(id, changeset.toMap());
        executor.pendingExecute(YqlStatement.update(type, model), model);
    }

    @Override
    public T insert(T t) {
        T entityToSave = t.preSave();
        executor.pendingExecute(YqlStatement.insert(type), entityToSave);
        executor.getTransactionLocal().firstLevelCache().put(entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
        return t;
    }

    @Override
    public T save(T t) {
        T entityToSave = t.preSave();
        executor.pendingExecute(YqlStatement.save(type), entityToSave);
        executor.getTransactionLocal().firstLevelCache().put(entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
        return t;
    }

    @Override
    public void delete(Entity.Id<T> id) {
        executor.pendingExecute(YqlStatement.delete(type), id);
        executor.getTransactionLocal().firstLevelCache().putEmpty(id);
        executor.getTransactionLocal().projectionCache().delete(id);
    }

    /**
     * Migrates the specified entity and its projections, if any. Does nothing if the entity does not exist.
     * <br>
     * If the entity has projections, its {@link Entity#createProjections() createProjections()}
     * method <strong>MUST NOT</strong> fail when called on a raw, non-{@link Entity#postLoad() post-loaded} entity.
     *
     * @param id   entity ID
     * @param <ID> entity ID type
     */
    public <ID extends Id<T>> void migrate(ID id) {
        List<T> foundRaw = executor.execute(YqlStatement.find(type), id);
        if (foundRaw.isEmpty()) {
            return;
        }
        T rawEntity = foundRaw.get(0);
        T entityToSave = rawEntity.postLoad().preSave();
        executor.pendingExecute(YqlStatement.save(type), entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
    }

    @Override
    @NonNull
    public T postLoad(T e) {
        T e1 = e.postLoad();
        if (e1 != e) {
            executor.getTransactionLocal().log().debug("    postLoad(%s) has diff", e1.getId());
        }
        executor.getTransactionLocal().firstLevelCache().put(e1);
        executor.getTransactionLocal().projectionCache().load(e1);
        return e1;
    }

    public interface QueryExecutor {
        <PARAMS, RESULT> List<RESULT> execute(Statement<PARAMS, RESULT> statement, PARAMS params);

        <PARAMS, RESULT> Stream<RESULT> executeScanQuery(Statement<PARAMS, RESULT> statement, PARAMS params);

        <PARAMS> void pendingExecute(Statement<PARAMS, ?> statement, PARAMS value);

        default <IN> void bulkUpsert(BulkMapper<IN> mapper, List<IN> input, BulkParams params) {
            throw new UnsupportedOperationException();
        }

        <IN, OUT> Stream<OUT> readTable(ReadTableMapper<IN, OUT> mapper, ReadTableParams<IN> params);

        TransactionLocal getTransactionLocal();
    }

    public static class CheckingQueryExecutor implements QueryExecutor {
        private final QueryExecutor delegate;
        private final Tx originTx;

        public CheckingQueryExecutor(QueryExecutor delegate) {
            this.delegate = delegate;
            this.originTx = Tx.Current.exists() ? Tx.Current.get() : null;
        }

        private void check() {
            Tx.checkSameTx(originTx);
        }

        @Override
        public <PARAMS, RESULT> List<RESULT> execute(Statement<PARAMS, RESULT> statement, PARAMS params) {
            check();
            return delegate.execute(statement, params);
        }

        @Override
        public <PARAMS, RESULT> Stream<RESULT> executeScanQuery(Statement<PARAMS, RESULT> statement, PARAMS params) {
            return delegate.executeScanQuery(statement, params);
        }

        @Override
        public <PARAMS> void pendingExecute(Statement<PARAMS, ?> statement, PARAMS value) {
            check();
            delegate.pendingExecute(statement, value);
        }

        @Override
        public <IN> void bulkUpsert(BulkMapper<IN> mapper, List<IN> input, BulkParams params) {
            check();
            delegate.bulkUpsert(mapper, input, params);
        }

        @Override
        public <IN, OUT> Stream<OUT> readTable(ReadTableMapper<IN, OUT> mapper, ReadTableParams<IN> params) {
            check();
            return delegate.readTable(mapper, params);
        }

        @Override
        public TransactionLocal getTransactionLocal() {
            check();
            return delegate.getTransactionLocal();
        }
    }

    public <ID extends Id<T>> void updateIn(Collection<ID> ids, Changeset changeset) {
        var params = new UpdateInStatement.UpdateInStatementInput<>(ids, changeset.toMap());

        executor.pendingExecute(
                new UpdateInStatement<>(EntitySchema.of(type), EntitySchema.of(type), params),
                params
        );
    }
}
