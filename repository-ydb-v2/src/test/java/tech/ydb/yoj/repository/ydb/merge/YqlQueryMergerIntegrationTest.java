package tech.ydb.yoj.repository.ydb.merge;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.RepositoryTestSupport;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.TestDbImpl;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.ydb.TestYdbRepository;
import tech.ydb.yoj.repository.ydb.YdbEnvAndTransportRule;
import tech.ydb.yoj.repository.ydb.util.RandomUtils;

public class YqlQueryMergerIntegrationTest extends RepositoryTestSupport {
    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    protected TestDb db;

    @Override
    public void setUp() {
        super.setUp();
        this.db = new TestDbImpl<>(this.repository);
    }

    @Override
    public void tearDown() {
        this.db = null;
        super.tearDown();
    }

    @Override
    protected final Repository createRepository() {
        return TestEntities.init(new TestYdbRepository(ydbEnvAndTransport.getYdbConfig(), ydbEnvAndTransport.getGrpcTransport()));
    }

    @Test
    public void insertAfterFindByIdWorks() {
        db.tx(() -> {
            Project.Id id = new Project.Id(RandomUtils.nextString(10));

            Assert.assertNull(db.projects().find(id));

            db.projects().insert(new Project(id, "project-1"));
        });
    }
}
