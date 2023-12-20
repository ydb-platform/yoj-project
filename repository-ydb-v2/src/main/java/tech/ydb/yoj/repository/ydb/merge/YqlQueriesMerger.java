package tech.ydb.yoj.repository.ydb.merge;

import tech.ydb.yoj.repository.ydb.YdbRepository;

import java.util.List;

public interface YqlQueriesMerger {
    void onNext(YdbRepository.Query<?> query);

    List<YdbRepository.Query<?>> getQueries();
}
