package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@With
public class Book implements Entity<Book> {
    Id id;
    int version;
    String title;
    List<String> authors;

    public Book updateTitle(String title) {
        return withTitle(title).withVersion(version + 1);
    }

    @Override
    public List<Entity<?>> createProjections() {
        return Stream.concat(
                Optional.ofNullable(title).map(t -> new ByTitle(new ByTitle.Id(t, id))).stream(),
                authors.stream().map(a -> new ByAuthor(new ByAuthor.Id(a, id)))
        ).collect(Collectors.toList());
    }

    @Value
    public static class Id implements Entity.Id<Book> {
        String id;
    }

    @Value
    public static class ByTitle implements Entity<ByTitle> {
        Id id;

        @Value
        public static class Id implements Entity.Id<ByTitle> {
            String title;
            Book.Id id;
        }
    }

    @Value
    public static class ByAuthor implements Entity<ByAuthor> {
        Id id;

        @Value
        public static class Id implements Entity.Id<ByAuthor> {
            String author;
            Book.Id id;
        }
    }

    @Value
    public static class TitleViewId implements Table.ViewId<Book> {
        Id id;
        String title;
    }
}
