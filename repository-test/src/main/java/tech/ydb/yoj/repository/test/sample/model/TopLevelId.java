package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

@Value
public class TopLevelId implements Entity.Id<EntityWithTopLevelId> {
    String value;
}
