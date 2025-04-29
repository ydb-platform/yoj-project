package tech.ydb.yoj.repository.ydb.merge;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

@InternalApi
public class QueriesMerger {
    private final Supplier<YqlQueriesMerger> factory;

    private QueriesMerger(Supplier<YqlQueriesMerger> factory) {
        this.factory = factory;
    }

    public List<YdbRepository.Query<?>> merge(YdbRepository.Query<?> first, YdbRepository.Query<?>... others) {
        return merge(concat(Stream.of(first), Stream.of(others)).collect(toList()));
    }

    public List<YdbRepository.Query<?>> merge(List<YdbRepository.Query<?>> pendingWrites) {
        List<YdbRepository.Query<?>> res = new ArrayList<>();

        YqlQueriesMerger currentMerger = factory.get();
        for (YdbRepository.Query<?> query : pendingWrites) {
            if (query.getStatement() instanceof YqlStatement) {
                currentMerger.onNext(query);
            } else {
                res.addAll(currentMerger.getQueries());
                res.add(query);
                currentMerger = factory.get();
            }
        }
        res.addAll(currentMerger.getQueries());

        return res;
    }

    public static QueriesMerger create(RepositoryCache cache) {
        return new QueriesMerger(() -> new ByEntityYqlQueriesMerger(cache));
    }
}
