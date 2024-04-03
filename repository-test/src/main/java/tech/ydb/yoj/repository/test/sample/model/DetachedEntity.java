package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.repository.db.RecordEntity;

public record DetachedEntity(DetachedEntityId id) implements RecordEntity<DetachedEntity> {
}
