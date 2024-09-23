package tech.ydb.yoj.repository.test.entity;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.sample.model.Book;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.BytePkEntity;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.DetachedEntity;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity;
import tech.ydb.yoj.repository.test.sample.model.NetworkAppliance;
import tech.ydb.yoj.repository.test.sample.model.NonDeserializableEntity;
import tech.ydb.yoj.repository.test.sample.model.Primitive;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.test.sample.model.Referring;
import tech.ydb.yoj.repository.test.sample.model.Supabubble;
import tech.ydb.yoj.repository.test.sample.model.Supabubble2;
import tech.ydb.yoj.repository.test.sample.model.Team;
import tech.ydb.yoj.repository.test.sample.model.TypeFreak;
import tech.ydb.yoj.repository.test.sample.model.UniqueProject;
import tech.ydb.yoj.repository.test.sample.model.UpdateFeedEntry;
import tech.ydb.yoj.repository.test.sample.model.VersionedAliasedEntity;
import tech.ydb.yoj.repository.test.sample.model.VersionedEntity;
import tech.ydb.yoj.repository.test.sample.model.WithUnflattenableField;

import java.util.List;

public final class TestEntities {
    private TestEntities() {
    }

    @SuppressWarnings("rawtypes")
    public static final List<Class<? extends Entity>> ALL = List.of(
            Project.class, UniqueProject.class, TypeFreak.class, Complex.class, Referring.class, Primitive.class,
            Book.class, Book.ByAuthor.class, Book.ByTitle.class,
            LogEntry.class, Team.class,
            BytePkEntity.class,
            EntityWithValidation.class,
            Bubble.class,
            IndexedEntity.class,
            Supabubble.class,
            Supabubble2.class,
            NonDeserializableEntity.class,
            WithUnflattenableField.class,
            UpdateFeedEntry.class,
            NetworkAppliance.class,
            VersionedEntity.class,
            VersionedAliasedEntity.class,
            DetachedEntity.class,
            MultiWrappedEntity.class
    );

    @SuppressWarnings("unchecked")
    public static Repository init(@NonNull Repository repository) {
        repository.createTablespace();
        ALL.forEach(entityClass -> repository.schema(entityClass).create());

        return repository;
    }
}
