package tech.ydb.yoj.repository.test.entity;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.test.sample.model.BadlyWrappedEntity;
import tech.ydb.yoj.repository.test.sample.model.Book;
import tech.ydb.yoj.repository.test.sample.model.Bubble;
import tech.ydb.yoj.repository.test.sample.model.BytePkEntity;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.DetachedEntity;
import tech.ydb.yoj.repository.test.sample.model.EntityWithValidation;
import tech.ydb.yoj.repository.test.sample.model.EnumEntity;
import tech.ydb.yoj.repository.test.sample.model.IndexedEntity;
import tech.ydb.yoj.repository.test.sample.model.LogEntry;
import tech.ydb.yoj.repository.test.sample.model.MigrationEntity;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity;
import tech.ydb.yoj.repository.test.sample.model.MultiWrappedEntity2;
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
import tech.ydb.yoj.repository.test.sample.model.annotations.UniqueEntity;
import tech.ydb.yoj.repository.test.sample.model.annotations.UniqueEntityNative;

import java.util.List;

public final class TestEntities {
    public static final TableDescriptor<UniqueProject> SECOND_UNIQUE_PROJECT_TABLE = new TableDescriptor<>(
            UniqueProject.class, "second_uniq_project_table"
    );

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
            MultiWrappedEntity.class,
            MultiWrappedEntity2.class,
            BadlyWrappedEntity.class,
            UniqueEntity.class,
            UniqueEntityNative.class,
            EnumEntity.class,
            MigrationEntity.class
    );

    public static final List<TableDescriptor<?>> ALL_TABLE_DESCRIPTORS = List.of(
            SECOND_UNIQUE_PROJECT_TABLE
    );

    @SuppressWarnings("unchecked")
    public static Repository init(@NonNull Repository repository) {
        repository.createTablespace();
        ALL.forEach(entityClass -> repository.schema(entityClass).create());

        for (TableDescriptor<?> tableDescriptor : ALL_TABLE_DESCRIPTORS) {
            repository.schema(tableDescriptor).create();
        }

        return repository;
    }
}
