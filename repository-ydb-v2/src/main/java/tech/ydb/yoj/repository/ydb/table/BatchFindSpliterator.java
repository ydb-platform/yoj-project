package tech.ydb.yoj.repository.ydb.table;

import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.ydb.yql.YqlLimit;
import tech.ydb.yoj.repository.ydb.yql.YqlOrderBy;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

abstract class BatchFindSpliterator<R, T extends Entity<T>, ID extends Entity.Id<T>> implements Spliterator<R> {
    private final YqlOrderBy orderById;
    private final YqlLimit top;
    private final int batchSize;
    private final EntityIdSchema<ID> idSchema;

    private List<YqlPredicate> initialPartialPredicates = List.of();
    private List<Schema.JavaFieldValue> lastPartialId = List.of();
    private List<R> remainingItems = List.of();
    private boolean finished = false;

    protected abstract ID getId(R r);

    protected abstract List<R> find(YqlStatementPart<?> part, YqlStatementPart<?>... otherParts);

    BatchFindSpliterator(EntityIdSchema<ID> idSchema, ID partial, int batchSize) {
        this.batchSize = batchSize;
        this.idSchema = idSchema;
        this.orderById = YqlOrderBy.orderBy(this.idSchema
                .flattenFields().stream()
                .map(s -> new YqlOrderBy.SortKey(s.getPath(), YqlOrderBy.SortOrder.ASC))
                .collect(toList())
        );
        this.top = YqlLimit.top(batchSize);
        if (partial != null) {
            Range<ID> range = Range.create(this.idSchema, partial);
            Map<JavaField, Object> eqMap = range.getEqMap();
            this.initialPartialPredicates = this.idSchema
                    .flattenFields().stream()
                    .filter(eqMap::containsKey)
                    .map(f -> YqlPredicate.eq(f.getPath(), eqMap.get(f)))
                    .collect(toList());
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super R> action) {
        List<R> result = remainingItems;
        while (result.isEmpty() && !finished) {
            result = next();

            if (!result.isEmpty()) {
                R lastResult = result.get(result.size() - 1);
                lastPartialId = idSchema.flattenToList(getId(lastResult));
            }

            if (result.size() < batchSize && !lastPartialId.isEmpty()) {
                lastPartialId.remove(lastPartialId.size() - 1);
            }

            finished = !(lastPartialId.size() > initialPartialPredicates.size());
        }

        boolean foundSomething = !result.isEmpty();
        if (foundSomething) {
            action.accept(result.get(0));
        }
        remainingItems = foundSomething ? result.subList(1, result.size()) : List.of();

        return foundSomething;
    }

    private List<R> next() {
        if (!lastPartialId.isEmpty() && lastPartialId.size() <= initialPartialPredicates.size()) {
            // We need this short-circuiting because certain versions of YDB had a bug for queries like
            // SELECT * FROM table WHERE id = 'id' AND id > 'id'
            return List.of();
        }

        List<YqlPredicate> predicates = new ArrayList<>(initialPartialPredicates);
        for (int i = 0; i < lastPartialId.size(); i++) {
            Schema.JavaFieldValue e = lastPartialId.get(i);
            predicates.add(i == lastPartialId.size() - 1
                    ? YqlPredicate.gt(e.getFieldPath(), e.getValue())
                    : YqlPredicate.eq(e.getFieldPath(), e.getValue()));
        }

        return find(YqlPredicate.and(predicates), orderById, top);
    }

    @Override
    public Spliterator<R> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | NONNULL;
    }
}
