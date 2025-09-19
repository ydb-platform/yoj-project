package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.RecordEntity;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.projection.EntityWithProjections;
import tech.ydb.yoj.repository.db.projection.ProjectionCollection;

import java.util.List;

@With
public record Book(
        Id id,
        int version,
        String title,
        List<String> authors
) implements RecordEntity<Book>, EntityWithProjections<Book> {
    public Book updateTitle(String title) {
        return withTitle(title).withVersion(version + 1);
    }

    @Override
    public ProjectionCollection collectProjections() {
        return ProjectionCollection.builder()
                .addEntityIfNotNull(title, t -> new ByTitle(new ByTitle.Id(t, id)))
                .addAllEntities(authors.stream().map(a -> new ByAuthor(new ByAuthor.Id(a, id))))
                .build();
    }

    public record Id(String id) implements Entity.Id<Book> {
    }

    public record ByTitle(Id id) implements RecordEntity<ByTitle> {
        public record Id(
                @NonNull
                String title,

                Book.Id id
        ) implements Entity.Id<ByTitle> {
        }
    }

    public record ByAuthor(Id id) implements RecordEntity<ByAuthor> {
        public record Id(
                String author,
                Book.Id id
        ) implements Entity.Id<ByAuthor> {
        }
    }

    public record TitleViewId(
            Id id,
            String title
    ) implements Table.RecordViewId<Book> {
    }
}
