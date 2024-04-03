package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.repository.db.Entity;

public record DetachedEntityId(String value) implements Entity.Id<DetachedEntity> {
}
