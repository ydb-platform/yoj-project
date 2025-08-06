package tech.ydb.yoj.repository.ydb.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Entity.Id;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.db.cache.FirstLevelCache;
import tech.ydb.yoj.repository.db.cache.TransactionLocal;
import tech.ydb.yoj.repository.db.internal.TableQueryImpl;
import tech.ydb.yoj.repository.db.readtable.ReadTableParams;
import tech.ydb.yoj.repository.db.statement.Changeset;
import tech.ydb.yoj.repository.ydb.bulk.BulkMapper;
import tech.ydb.yoj.repository.ydb.bulk.BulkMapperImpl;
import tech.ydb.yoj.repository.ydb.readtable.EntityIdKeyMapper;
import tech.ydb.yoj.repository.ydb.readtable.ReadTableMapper;
import tech.ydb.yoj.repository.ydb.statement.CountAllStatement;
import tech.ydb.yoj.repository.ydb.statement.DeleteAllStatement;
import tech.ydb.yoj.repository.ydb.statement.DeleteByIdStatement;
import tech.ydb.yoj.repository.ydb.statement.FindAllYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.FindInStatement;
import tech.ydb.yoj.repository.ydb.statement.FindRangeStatement;
import tech.ydb.yoj.repository.ydb.statement.FindStatement;
import tech.ydb.yoj.repository.ydb.statement.FindYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.InsertYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.repository.ydb.statement.UpdateByIdStatement;
import tech.ydb.yoj.repository.ydb.statement.UpdateInStatement;
import tech.ydb.yoj.repository.ydb.statement.UpdateModel;
import tech.ydb.yoj.repository.ydb.statement.UpsertYqlStatement;
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
    private final Class<T> type;
    private final TableDescriptor<T> tableDescriptor;
    private final EntitySchema<T> schema;
    private final QueryExecutor executor;

    public YdbTable(Class<T> type, QueryExecutor executor) {
        this.type = type;
        this.executor = new CheckingQueryExecutor(executor);
        this.schema = EntitySchema.of(type);
        this.tableDescriptor = TableDescriptor.from(schema);
    }

    protected YdbTable(QueryExecutor executor) {
        this.type = resolveEntityType();
        this.executor = new CheckingQueryExecutor(executor);
        this.schema = EntitySchema.of(type);
        this.tableDescriptor = TableDescriptor.from(schema);
    }

    public YdbTable(TableDescriptor<T> tableDescriptor, QueryExecutor executor) {
        this.type = tableDescriptor.entityType();
        this.executor = new CheckingQueryExecutor(executor);
        this.schema = EntitySchema.of(type);
        this.tableDescriptor = tableDescriptor;
    }

    @Override
    public final Class<T> getType() {
        return type;
    }

    @Override
    public final TableDescriptor<T> getTableDescriptor() {
        return tableDescriptor;
    }

    public final EntitySchema<T> getSchema() {
        return schema;
    }

    @InternalApi
    protected final QueryExecutor getExecutor() {
        return executor;
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
        var statement = new FindAllYqlStatement<>(tableDescriptor, schema, schema);
        return postLoad(executor.execute(statement, null));
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
        return StreamSupport.stream(new BatchFindSpliterator<>(schema.getIdSchema(), partial, batchSize) {
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
        return StreamSupport.stream(new BatchFindSpliterator<>(schema.getIdSchema(), partial, batchSize) {
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
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = new FindAllYqlStatement<>(tableDescriptor, schema, viewSchema);
        return executor.execute(statement, null);
    }

    @Override
    public void deleteAll() {
        executor.pendingExecute(new DeleteAllStatement<>(tableDescriptor, schema), null);
    }

    @Override
    public void bulkUpsert(List<T> input, BulkParams params) {
        var mapper = new BulkMapperImpl<>(tableDescriptor, schema);
        executor.bulkUpsert(mapper, input, params);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<T> readTable(ReadTableParams<ID> params) {
        ReadTableMapper<ID, T> mapper = new EntityIdKeyMapper<>(tableDescriptor, schema, schema);
        return readTableStream(mapper, params)
                .map(T::postLoad);
    }

    @Override
    public <ID extends Entity.Id<T>> Stream<ID> readTableIds(ReadTableParams<ID> params) {
        EntityIdSchema<ID> idSchema = schema.getIdSchema();
        ReadTableMapper<ID, ID> mapper = new EntityIdKeyMapper<>(tableDescriptor, schema, idSchema);
        return readTableStream(mapper, params);
    }

    @Override
    public <V extends ViewId<T>, ID extends Id<T>> Stream<V> readTable(Class<V> viewClass, ReadTableParams<ID> params) {
        ViewSchema<V> viewSchema = schema.getViewSchema(viewClass);
        ReadTableMapper<ID, V> mapper = new EntityIdKeyMapper<>(tableDescriptor, schema, viewSchema);
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
        return executor.getTransactionLocal().firstLevelCache(tableDescriptor).get(id, __ -> {
            var statement = new FindYqlStatement<>(tableDescriptor, schema, schema);
            List<T> res = postLoad(executor.execute(statement, id));
            return res.isEmpty() ? null : res.get(0);
        });
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Set<ID> ids) {
        return TableQueryImpl.find(this, getFirstLevelCache(), ids);
    }

    @Override
    public <V extends View> V find(Class<V> viewType, Entity.Id<T> id) {
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = new FindYqlStatement<>(tableDescriptor, schema, viewSchema);
        List<V> res = executor.execute(statement, id);
        return res.isEmpty() ? null : res.get(0);
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> find(Range<ID> range) {
        var statement = new FindRangeStatement<>(tableDescriptor, schema, schema, range);
        return postLoad(executor.execute(statement, range));
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Range<ID> range) {
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = new FindRangeStatement<>(tableDescriptor, schema, viewSchema, range);
        return executor.execute(statement, range);
    }

    @Override
    public <V extends View, ID extends Entity.Id<T>> List<V> find(Class<V> viewType, Set<ID> ids) {
        return find(viewType, ids, null, defaultOrder(schema), null);
    }

    public final List<T> find(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return find(toList(part, otherParts));
    }

    public List<T> find(Collection<? extends YqlStatementPart<?>> parts) {
        var statement = FindStatement.from(tableDescriptor, schema, schema, parts, false);
        return postLoad(executor.execute(statement, parts));
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
    public List<T> find(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit, @Nullable Long offset
    ) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return find(statements);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit, @Nullable Long offset
    ) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return findIds(statements);
    }

    @Override
    public <V extends View> List<V> find(
            Class<V> viewType,
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit, @Nullable Long offset,
            boolean distinct
    ) {
        List<YqlStatementPart<?>> statements = buildStatementParts(indexName, filter, orderBy, limit, offset);

        return find(viewType, statements, distinct);
    }

    @Override
    public <ID extends Id<T>> List<T> find(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }
        var isPartialIdMode = ids.iterator().next().isPartial();
        List<T> found = postLoad(findUncached(ids, filter, orderBy, limit));
        if (!isPartialIdMode && ids.size() > found.size()) {
            Set<Id<T>> foundIds = found.stream().map(Entity::getId).collect(toSet());
            FirstLevelCache<T> cache = executor.getTransactionLocal().firstLevelCache(tableDescriptor);
            Sets.difference(ids, foundIds).forEach(cache::putEmpty);
        }
        return found;
    }

    @Override
    public <ID extends Entity.Id<T>> List<T> findUncached(
            Set<ID> ids,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }
        var statement = FindInStatement.from(tableDescriptor, schema, schema, ids, filter, orderBy, limit);
        return executor.execute(statement, ids);
    }

    @Override
    public <V extends View, ID extends Id<T>> List<V> find(
            Class<V> viewType,
            Set<ID> ids,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (ids.isEmpty()) {
            return List.of();
        }
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = FindInStatement.from(
                tableDescriptor, schema, viewSchema, ids, filter, orderBy, limit
        );
        return executor.execute(statement, ids);
    }

    @Override
    public <K> List<T> find(
            String indexName,
            Set<K> keys,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit) {
        if (keys.isEmpty()) {
            return List.of();
        }
        var statement = FindInStatement.from(
                tableDescriptor, schema, schema, indexName, keys, filter, orderBy, limit
        );
        return postLoad(executor.execute(statement, keys));
    }

    @Override
    public <V extends View, K> List<V> find(
            Class<V> viewType,
            String indexName,
            Set<K> keys,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit
    ) {
        if (keys.isEmpty()) {
            return List.of();
        }
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = FindInStatement.from(
                tableDescriptor, schema, viewSchema, indexName, keys, filter, orderBy, limit
        );
        return executor.execute(statement, keys);
    }

    public static <T extends Entity<T>> List<YqlStatementPart<? extends YqlStatementPart<?>>> buildStatementParts(
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit, @Nullable Long offset
    ) {
        return buildStatementParts(null, filter, orderBy, limit, offset);
    }

    public static <T extends Entity<T>> List<YqlStatementPart<? extends YqlStatementPart<?>>> buildStatementParts(
            @Nullable String indexName,
            @Nullable FilterExpression<T> filter, @Nullable OrderExpression<T> orderBy,
            @Nullable Integer limit, @Nullable Long offset
    ) {
        YqlPredicate yqlFilter = filter == null ? null : YqlListingQuery.toYqlPredicate(filter);
        YqlOrderBy yqlOrderBy = orderBy == null ? null : YqlListingQuery.toYqlOrderBy(orderBy);

        YqlLimit yqlLimit;
        long offsetOrZero = offset == null ? 0L : offset;
        if (limit == null) {
            Preconditions.checkArgument(offsetOrZero == 0L, "nonzero offset without limit is not supported");
            yqlLimit = null;
        } else {
            yqlLimit = YqlLimit.range(offsetOrZero, offsetOrZero + limit);
        }

        YqlView yqlView = indexName == null ? null : YqlView.index(indexName);

        return Stream.of(yqlFilter, yqlView, yqlOrderBy, yqlLimit)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public long count(YqlStatementPart<?>... parts) {
        List<YqlStatementPart<?>> partsList = asList(parts);
        var statement = new CountAllStatement<>(tableDescriptor, schema, partsList);
        return executor.execute(statement, partsList).get(0).getCount();
    }

    public <V extends View> List<V> find(Class<V> viewType, YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return find(viewType, toList(part, otherParts), false);
    }

    public <V extends View> List<V> find(Class<V> viewType, Collection<? extends YqlStatementPart<?>> parts, boolean distinct) {
        ViewSchema<V> viewSchema = schema.getViewSchema(viewType);
        var statement = FindStatement.from(tableDescriptor, schema, viewSchema, parts, distinct);
        return executor.execute(statement, parts);
    }

    public <ID extends Entity.Id<T>> List<ID> findIds(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts) {
        return findIds(toList(part, otherParts));
    }

    private <ID extends Entity.Id<T>> List<ID> findIds(Collection<? extends YqlStatementPart<?>> parts) {
        EntityIdSchema<ID> idSchema = schema.getIdSchema();
        var statement = FindStatement.from(tableDescriptor, schema, idSchema, parts, false);
        return executor.execute(statement, parts);
    }

    @Override
    public <ID extends Entity.Id<T>> List<ID> findIds(Range<ID> range) {
        EntityIdSchema<ID> idSchema = schema.getIdSchema();
        var statement = new FindRangeStatement<>(tableDescriptor, schema, idSchema, range);
        return executor.execute(statement, range);
    }

    @Override
    public <ID extends Id<T>> List<ID> findIds(Set<ID> partialIds) {
        if (partialIds.isEmpty()) {
            return List.of();
        }
        OrderExpression<T> order = defaultOrder(schema);
        EntityIdSchema<ID> idSchema = schema.getIdSchema();
        var statement = FindInStatement.from(tableDescriptor, schema, idSchema, partialIds, null, order, null);
        return executor.execute(statement, partialIds);
    }

    @Override
    @Deprecated
    public void update(Entity.Id<T> id, Changeset changeset) {
        UpdateModel.ById<Id<T>> model = new UpdateModel.ById<>(id, changeset.toMap());
        executor.pendingExecute(new UpdateByIdStatement<>(tableDescriptor, schema, model), model);
        executor.getTransactionLocal().firstLevelCache(tableDescriptor).remove(id);
    }

    @Override
    public T insert(T t) {
        T entityToSave = t.preSave();
        executor.pendingExecute(new InsertYqlStatement<>(tableDescriptor, schema), entityToSave);
        executor.getTransactionLocal().firstLevelCache(tableDescriptor).put(entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
        return t;
    }

    @Override
    public T save(T t) {
        T entityToSave = t.preSave();
        executor.pendingExecute(new UpsertYqlStatement<>(tableDescriptor, schema), entityToSave);
        executor.getTransactionLocal().firstLevelCache(tableDescriptor).put(entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
        return t;
    }

    @Override
    public void delete(Entity.Id<T> id) {
        executor.pendingExecute(new DeleteByIdStatement<>(tableDescriptor, schema), id);
        executor.getTransactionLocal().firstLevelCache(tableDescriptor).putEmpty(id);
        executor.getTransactionLocal().projectionCache().delete(id);
    }

    /**
     * Migrates the specified entity and its projections, if any. Does nothing if the entity does not exist.
     * <br>
     * If the entity has projections, its {@link Entity#createProjections() createProjections()}
     * method <strong>MUST NOT</strong> fail when called on a raw, non-{@link Entity#postLoad() post-loaded} entity.
     *
     * @deprecated This method will be removed in YOJ 3.0.0, along with projection-related logic; because without projection magic&trade;,
     * it will works the same as {@code Table.find()} + {@code Table.save()} (which will apply the {@code Entity.postLoad()} and
     * {@code Entity.preSave()} and save the entity only if it has been changed by these calls).
     *
     * @see <a href="https://github.com/ydb-platform/yoj-project/issues/77">#77</a>
     *
     * @param id   entity ID
     * @param <ID> entity ID type
     */
    @Deprecated(forRemoval = true)
    public <ID extends Id<T>> void migrate(ID id) {
        var statement = new FindYqlStatement<>(tableDescriptor, schema, schema);
        List<T> foundRaw = executor.execute(statement, id);
        if (foundRaw.isEmpty()) {
            return;
        }
        T rawEntity = foundRaw.get(0);
        T entityToSave = rawEntity.postLoad().preSave();
        executor.pendingExecute(new UpsertYqlStatement<>(tableDescriptor, schema), entityToSave);
        executor.getTransactionLocal().projectionCache().save(entityToSave);
    }

    public FirstLevelCache<T> getFirstLevelCache() {
        return executor.getTransactionLocal().firstLevelCache(tableDescriptor);
    }

    @Override
    @NonNull
    public T postLoad(T e) {
        T e1 = e.postLoad();
        executor.getTransactionLocal().firstLevelCache(tableDescriptor).put(e1);
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

    /**
     * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
     * have specific requirements.
     * <p>Blind updates disrupt query merging mechanism, so you typically won't able to run multiple blind update statements
     * in the same transaction, or interleave them with upserts ({@code Table.save()}) and inserts.
     * <p>Blind updates also do not update projections because they do not load the entity before performing the update;
     * this can cause projections to be inconsistent with the main entity.
     */
    @Deprecated
    public <ID extends Id<T>> void updateIn(Collection<ID> ids, Changeset changeset) {
        var params = new UpdateInStatement.UpdateInStatementInput<>(ids, changeset.toMap());

        executor.pendingExecute(
                new UpdateInStatement<>(tableDescriptor, schema, schema, params),
                params
        );
    }
}
