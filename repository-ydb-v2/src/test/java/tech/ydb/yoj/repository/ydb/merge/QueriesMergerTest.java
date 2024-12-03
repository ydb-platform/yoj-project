package tech.ydb.yoj.repository.ydb.merge;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.cache.RepositoryCache;
import tech.ydb.yoj.repository.db.cache.RepositoryCacheImpl;
import tech.ydb.yoj.repository.test.sample.model.Primitive;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.statement.InsertYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.Statement;
import tech.ydb.yoj.repository.ydb.statement.YqlStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueriesMergerTest {
    @Test
    public void mergeInsertQueries() {
        QueriesMerger merger = createMerger();

        List<YdbRepository.Query<?>> queries = new ArrayList<>();
        getProjects().forEach(p -> queries.add(insert(p)));
        List<YdbRepository.Query<?>> result = merger.merge(queries);

        assertThat(result).hasSize(1);
    }

    @Test
    public void mergeFindInsertQueries() {
        RepositoryCacheImpl cache = new RepositoryCacheImpl();
        QueriesMerger merger = QueriesMerger.create(cache);

        List<YdbRepository.Query<?>> queries = new ArrayList<>();
        getProjects().forEach(p -> cache.put(new RepositoryCache.Key(p.getClass(), p.getId()), null));
        getProjects().forEach(p -> queries.add(insert(p)));

        merger.merge(queries);
    }

    @Test
    public void mergeInsertQueriesForTwoTables() {
        QueriesMerger merger = createMerger();

        List<YdbRepository.Query<?>> queries = new ArrayList<>();
        getProjects().forEach(p -> queries.add(insert(p)));
        getPrimitives().forEach(p -> queries.add(insert(p)));
        List<YdbRepository.Query<?>> result = merger.merge(queries);

        assertThat(result).hasSize(2);
    }

    @Test
    public void mergeUpsertQueriesForTwoTables() {
        QueriesMerger merger = createMerger();

        List<YdbRepository.Query<?>> queries = new ArrayList<>();
        getProjects().forEach(p -> queries.add(upsert(p)));
        getPrimitives().forEach(p -> queries.add(upsert(p)));
        List<YdbRepository.Query<?>> result = merger.merge(queries);

        assertThat(result).hasSize(2);
    }

    @Test
    public void mergeDeleteInsertQueries() {
        QueriesMerger merger = createMerger();

        Project p = new Project(new Project.Id("1"), "new project");
        List<YdbRepository.Query<?>> result = merger.merge(
                delete(p),
                insert(p));

        assertThat(result).hasSize(1);
        Assertions.assertThat(result.get(0).getStatement().getQueryType()).isEqualTo(Statement.QueryType.UPSERT);
    }

    @Test
    public void mergeInsertDeleteQueries() {
        QueriesMerger merger = createMerger();

        Project p = new Project(new Project.Id("1"), "new project");
        List<YdbRepository.Query<?>> result = merger.merge(
                insert(p),
                delete(p));

        assertThat(result).hasSize(2);
        Assertions.assertThat(result.get(0).getStatement().getQueryType()).isEqualTo(Statement.QueryType.INSERT);
        assertThat(result.get(0).getValues()).isEqualTo(Collections.singletonList(p));
        Assertions.assertThat(result.get(1).getStatement().getQueryType()).isEqualTo(Statement.QueryType.DELETE);
        assertThat(result.get(1).getValues()).isEqualTo(Collections.singletonList(p.getId()));

    }

    @Test
    public void mergeInsertAndManyDeletesQueries() {
        QueriesMerger merger = createMerger();

        Project p = new Project(new Project.Id("1"), "new project");
        Project p2 = new Project(new Project.Id("2"), "new project2");
        Project p3 = new Project(new Project.Id("3"), "new project3");
        List<YdbRepository.Query<?>> result = merger.merge(
                delete(p2),
                insert(p),
                delete(p3),
                delete(p));

        assertThat(result).hasSize(2);
        Assertions.assertThat(result.get(0).getStatement().getQueryType()).isEqualTo(Statement.QueryType.INSERT);
        assertThat(result.get(0).getValues()).isEqualTo(Collections.singletonList(p));
        Assertions.assertThat(result.get(1).getStatement().getQueryType()).isEqualTo(Statement.QueryType.DELETE);
        //noinspection unchecked
        assertThat(result.get(1).getValues()).hasSize(3);
    }

    @Test
    public void mergeDeleteAllQueries() {
        QueriesMerger merger = createMerger();

        Project p = new Project(new Project.Id("1"), "new project");
        Project p2 = new Project(new Project.Id("2"), "new project2");
        Project p3 = new Project(new Project.Id("3"), "new project3");
        List<YdbRepository.Query<?>> result = merger.merge(
                insert(p),
                delete(p),
                insert(p2),
                upsert(p3),
                deleteAll(Project.class));

        assertThat(result).hasSize(1);
        Assertions.assertThat(result.get(0).getStatement().getQueryType()).isEqualTo(Statement.QueryType.DELETE_ALL);
    }

    private QueriesMerger createMerger() {
        return QueriesMerger.create(new RepositoryCacheImpl());
    }

    private <T extends Entity<T>> YdbRepository.Query<?> deleteAll(Class<T> clazz) {
        return new YdbRepository.Query<>(YqlStatement.deleteAll(clazz), null);
    }

    @SuppressWarnings("unchecked")
    private <T extends Entity<T>> YdbRepository.Query<?> upsert(T p) {
        return new YdbRepository.Query<>(YqlStatement.save((Class<T>) p.getClass()), p);
    }

    @SuppressWarnings("unchecked")
    private <T extends Entity<T>> YdbRepository.Query<?> insert(T p) {
        Class<T> type = (Class<T>) p.getClass();
        EntitySchema<T> schema = EntitySchema.of(type);
        return new YdbRepository.Query<>(new InsertYqlStatement<>(schema), p);
    }

    @SuppressWarnings("unchecked")
    private <T extends Entity<T>> YdbRepository.Query<?> find(T p) {
        return new YdbRepository.Query<>(YqlStatement.find((Class<T>) p.getClass()), p.getId());
    }

    @SuppressWarnings("unchecked")
    private <T extends Entity<T>> YdbRepository.Query<?> delete(T p) {
        return new YdbRepository.Query<>(YqlStatement.delete((Class<T>) p.getClass()), p.getId());
    }

    private ArrayList<Project> getProjects() {
        ArrayList<Project> projects = new ArrayList<>();
        projects.add(new Project(new Project.Id("1"), "first"));
        projects.add(new Project(new Project.Id("2"), "second"));
        projects.add(new Project(new Project.Id("3"), "third"));
        return projects;
    }

    private ArrayList<Primitive> getPrimitives() {
        ArrayList<Primitive> projects = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            projects.add(new Primitive(new Primitive.Id(i), i));
        }
        return projects;
    }
}
