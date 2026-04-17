package tech.ydb.yoj.repository.test.inmemory;

import org.junit.Test;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.bulk.BulkParams;
import tech.ydb.yoj.repository.test.RepositoryTest;
import tech.ydb.yoj.repository.test.sample.model.Project;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryRepositoryTest extends RepositoryTest<TestInMemoryRepository> {

    @Override
    protected Repository createDb() {
        return new TestInMemoryRepository();
    }

    @Test
    public void bulkUpsert() {
        Project.Id existingId = new Project.Id("existing");
        Project.Id newId = new Project.Id("new");

        db.tx(() -> db.projects().save(new Project(existingId, "before")));

        db.tx(() -> db.projects().bulkUpsert(
                List.of(
                        new Project(existingId, "after"),
                        new Project(newId, "created")
                ),
                BulkParams.DEFAULT
        ));

        List<Project> projects = db.tx(() -> db.projects().findAll());

        assertThat(projects).containsExactlyInAnyOrder(
                new Project(existingId, "after"),
                new Project(newId, "created")
        );
    }
}
