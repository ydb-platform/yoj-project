package tech.ydb.yoj.repository.ydb.merge;

import lombok.Value;
import lombok.With;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.statement.DeleteByIdStatement;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.repository.ydb.statement.UpsertYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InternalApi
public class ByEntityYqlQueriesMerger implements YqlQueriesMerger {
    private static final Logger log = LoggerFactory.getLogger(ByEntityYqlQueriesMerger.class);

    private static final Set<Statement.QueryType> SUPPORTED_QUERY_TYPES = new HashSet<>(Arrays.asList(
            Statement.QueryType.INSERT,
            Statement.QueryType.DELETE,
            Statement.QueryType.UPSERT,
            Statement.QueryType.UPDATE,
            Statement.QueryType.DELETE_ALL));
    private static final Map<TransitionKey, MergingState> transitionMap = createTransitionMap();

    private final Map<TableDescriptor<?>, TableState> states = new HashMap<>();
    private final RepositoryCache cache;

    ByEntityYqlQueriesMerger(RepositoryCache cache) {
        this.cache = cache;
    }

    @Override
    public void onNext(YdbRepository.Query<?> query) {
        Statement.QueryType queryType = query.getStatement().getQueryType();
        check(SUPPORTED_QUERY_TYPES.contains(queryType), "Unsupported query type: " + queryType);

        TableDescriptor<?> tableDescriptor = convertQueryToYqlStatement(query).getTableDescriptor();
        TableState tableState = states.computeIfAbsent(tableDescriptor, __ -> new TableState());
        if (queryType == Statement.QueryType.DELETE_ALL) {
            tableState.entityStates.clear();
            tableState.deleteAll = query;
            return;
        } else if (queryType == Statement.QueryType.UPDATE) {
            check(tableState.isEmpty(), "Update operation couldn't be after other modifications");
            tableState.update = query;
            return;
        }

        check(tableState.deleteAll == null && tableState.update == null,
                "Modifications after delete_all or update aren't allowed");
        EntityState state;
        Entity.Id<?> id = getEntityId(query);
        if (tableState.entityStates.containsKey(id)) {
            state = tableState.entityStates.get(id);
            MergingState oldMergingState = state.getState();
            state = state.withState(doTransition(oldMergingState, queryType, query));
            if (state.getState() != MergingState.INS_DEL) {
                YdbRepository.Query<?> replaceWith = query;
                if (oldMergingState == MergingState.DELETE && queryType == Statement.QueryType.INSERT) {
                    // DELETE, INSERT -> UPSERT
                    replaceWith = convertInsertToUpsert(query);
                }
                state = state.withQuery(replaceWith);
            }
        } else {
            state = new EntityState(query, doTransition(MergingState.INITIAL, queryType, query));
        }
        tableState.entityStates.put(id, state);
    }

    @Override
    public List<YdbRepository.Query<?>> getQueries() {
        Map<MergingState, List<YdbRepository.Query<?>>> queries = new HashMap<>();
        List<YdbRepository.Query<?>> specificQueries = new ArrayList<>();

        for (TableState tableState : states.values()) {
            if (tableState.deleteAll != null) {
                specificQueries.add(tableState.deleteAll);
            } else if (tableState.update != null) {
                specificQueries.add(tableState.update);
            } else {
                Map<MergingState, YdbRepository.Query<?>> curQueries = new HashMap<>();
                for (EntityState entityState : tableState.entityStates.values()) {
                    MergingState curState = entityState.state;
                    if (curState == MergingState.INS_DEL) {
                        updateCurQueries(curQueries, convertInsertToDelete(entityState.query), MergingState.DELETE);
                        curState = MergingState.INSERT;
                    } else if (needIgnoreQuery(entityState)) {
                        log.trace("Ignoring query: [{}]", entityState.query.getStatement());
                        continue;
                    }
                    updateCurQueries(curQueries, entityState.query, curState);
                }

                for (Map.Entry<MergingState, YdbRepository.Query<?>> entry : curQueries.entrySet()) {
                    queries.computeIfAbsent(entry.getKey(), __ -> new ArrayList<>()).add(entry.getValue());
                }
            }
        }

        List<YdbRepository.Query<?>> result = new ArrayList<>();
        addAllIfNonNull(result, queries.get(MergingState.INSERT));
        addAllIfNonNull(result, queries.get(MergingState.UPSERT));
        addAllIfNonNull(result, queries.get(MergingState.DELETE));
        result.addAll(specificQueries);
        return result;
    }

    private boolean needIgnoreQuery(EntityState entityState) {
        if (entityState.state == MergingState.UPSERT || entityState.state == MergingState.INSERT) {
            YqlStatement<?, ?, ?> srcStatement = convertQueryToYqlStatement(entityState.query);
            Class<?> entityClass = srcStatement.getInSchemaType();
            TableDescriptor<?> tableDescriptor = srcStatement.getTableDescriptor();
            Entity.Id<?> entityId = getEntityId(entityState.query);
            RepositoryCache.Key key = new RepositoryCache.Key(entityClass, tableDescriptor, entityId);

            if (entityState.state == MergingState.UPSERT) {
                boolean newValueEqualsCached = cache.get(key)
                        .map(entity -> entity.equals(entityState.query.getValues().get(0)))
                        .orElse(false);
                if (newValueEqualsCached) {
                    log.trace("New value {} is equal to cached value", entityState.query.getValues().get(0));
                }
                return newValueEqualsCached;
            } else if (cache.contains(key) && cache.get(key).isPresent()) { // INSERT case
                throw new EntityAlreadyExistsException("Entity " + entityId + " already exists");
            }
        }
        return false;
    }

    private void addAllIfNonNull(List<YdbRepository.Query<?>> result, List<YdbRepository.Query<?>> additional) {
        if (additional != null) {
            result.addAll(additional);
        }
    }

    private void updateCurQueries(Map<MergingState, YdbRepository.Query<?>> curQueries, YdbRepository.Query<?> newQuery, MergingState curState) {
        curQueries.computeIfPresent(curState, (__, q) -> q.merge(newQuery));
        curQueries.putIfAbsent(curState, newQuery);
    }

    private MergingState doTransition(MergingState state, Statement.QueryType nextQueryType, YdbRepository.Query<?> query) {
        if (state == MergingState.INSERT && nextQueryType == Statement.QueryType.INSERT) {
            throw new EntityAlreadyExistsException("Entity " + getEntityId(query) + " already exists");
        }
        MergingState nextState = transitionMap.get(new TransitionKey(state, nextQueryType));
        check(nextState != null, "Incorrect transition, from " + state + " by " + nextQueryType);
        return nextState;
    }

    private static <E extends Entity<E>> YdbRepository.Query<?> convertInsertToUpsert(YdbRepository.Query<?> query) {
        YqlStatement<?, E, ?> srcStatement = convertQueryToYqlStatement(query);
        EntitySchema<E> schema = srcStatement.getInSchema();
        TableDescriptor<E> tableDescriptor = srcStatement.getTableDescriptor();

        return new YdbRepository.Query<>(
                new UpsertYqlStatement<>(tableDescriptor, schema),
                query.getValues().get(0)
        );
    }

    private static <E extends Entity<E>> YdbRepository.Query<?> convertInsertToDelete(YdbRepository.Query<?> query) {
        YqlStatement<?, E, ?> srcStatement = convertQueryToYqlStatement(query);
        EntitySchema<E> schema = srcStatement.getInSchema();
        TableDescriptor<E> tableDescriptor = srcStatement.getTableDescriptor();

        return new YdbRepository.Query<>(
                new DeleteByIdStatement<>(tableDescriptor, schema),
                getEntityId(query)
        );
    }

    private static Entity.Id<?> getEntityId(YdbRepository.Query<?> query) {
        check(query.getValues().size() == 1, "Unsupported query");

        Object value = query.getValues().get(0);
        if (query.getStatement().getQueryType() == Statement.QueryType.DELETE) {
            return (Entity.Id<?>) value;
        } else {
            return ((Entity<?>) value).getId();
        }
    }

    private static <E extends Entity<E>> YqlStatement<?, E, ?> convertQueryToYqlStatement(YdbRepository.Query<?> query) {
        return (YqlStatement<?, E, ?>) query.getStatement();
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new YdbRepositoryException(message);
        }
    }

    private static Map<TransitionKey, MergingState> createTransitionMap() {
        Map<TransitionKey, MergingState> table = new HashMap<>();
        table.put(new TransitionKey(MergingState.INITIAL, Statement.QueryType.INSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.INITIAL, Statement.QueryType.UPSERT), MergingState.UPSERT);
        table.put(new TransitionKey(MergingState.INITIAL, Statement.QueryType.DELETE), MergingState.DELETE);

        table.put(new TransitionKey(MergingState.INSERT, Statement.QueryType.INSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.INSERT, Statement.QueryType.UPSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.INSERT, Statement.QueryType.DELETE), MergingState.INS_DEL);

        table.put(new TransitionKey(MergingState.INS_DEL, Statement.QueryType.INSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.INS_DEL, Statement.QueryType.UPSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.INS_DEL, Statement.QueryType.DELETE), MergingState.INS_DEL);

        table.put(new TransitionKey(MergingState.UPSERT, Statement.QueryType.INSERT), MergingState.INSERT);
        table.put(new TransitionKey(MergingState.UPSERT, Statement.QueryType.UPSERT), MergingState.UPSERT);
        table.put(new TransitionKey(MergingState.UPSERT, Statement.QueryType.DELETE), MergingState.DELETE);

        table.put(new TransitionKey(MergingState.DELETE, Statement.QueryType.INSERT), MergingState.UPSERT);
        table.put(new TransitionKey(MergingState.DELETE, Statement.QueryType.UPSERT), MergingState.UPSERT);
        table.put(new TransitionKey(MergingState.DELETE, Statement.QueryType.DELETE), MergingState.DELETE);

        return table;
    }

    @With
    @Value
    private static class EntityState {
        YdbRepository.Query<?> query;
        MergingState state;
    }

    private static class TableState {
        private final Map<Entity.Id<?>, EntityState> entityStates = new HashMap<>();
        private YdbRepository.Query<?> deleteAll;
        private YdbRepository.Query<?> update;

        public boolean isEmpty() {
            return entityStates.isEmpty() && update == null && deleteAll == null;
        }
    }

    @Value
    private static class TransitionKey {
        MergingState state;
        Statement.QueryType nextQueryType;
    }

    private enum MergingState {
        INITIAL,
        INSERT,
        INS_DEL,
        UPSERT,
        DELETE,
    }
}
