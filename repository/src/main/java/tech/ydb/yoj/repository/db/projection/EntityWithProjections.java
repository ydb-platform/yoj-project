package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.repository.db.Entity;

public interface EntityWithProjections<E extends EntityWithProjections<E>> extends Entity<E> {
    default ProjectionCollection collectProjections() {
        return ProjectionCollection.copyOf(Entity.super.createProjections());
    }
}
