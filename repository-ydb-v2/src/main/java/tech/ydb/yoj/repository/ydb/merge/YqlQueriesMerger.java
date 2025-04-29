package tech.ydb.yoj.repository.ydb.merge;

import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.repository.ydb.YdbRepository;

import java.util.List;

@InternalApi
public interface YqlQueriesMerger {
    void onNext(YdbRepository.Query<?> query);

    List<YdbRepository.Query<?>> getQueries();
}
