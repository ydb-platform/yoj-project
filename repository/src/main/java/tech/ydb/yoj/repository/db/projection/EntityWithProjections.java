package tech.ydb.yoj.repository.db.projection;

import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.repository.db.Entity;

/**
 * Base interface for {@link Entity entities} that can have projections.
 *
 * @param <E> self type
 */
@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
public interface EntityWithProjections<E extends EntityWithProjections<E>> extends Entity<E> {
    /**
     * Creates a collections of projections (=computed index entities) for this entity. The returned collection may be empty.
     * <p>The default implementation uses the result of {@link Entity#createProjections()}, for backwards compatibility.
     * <strong>It is highly recommended to construct a {@link ProjectionCollection} explicitly, instead.</strong>
     *
     * @return projections for this entity
     */
    @ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/issues/77")
    default ProjectionCollection collectProjections() {
        return ProjectionCollection.of(Entity.super.createProjections());
    }
}
