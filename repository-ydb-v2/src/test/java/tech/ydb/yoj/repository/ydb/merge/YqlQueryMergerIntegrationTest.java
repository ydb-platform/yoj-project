package tech.ydb.yoj.repository.ydb.merge;

import org.junit.ClassRule;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.ScopedTxManager;
import tech.ydb.yoj.repository.test.RepositoryTestSupport;
import tech.ydb.yoj.repository.test.entity.TestEntities;
import tech.ydb.yoj.repository.test.sample.TestDb;
import tech.ydb.yoj.repository.test.sample.model.Project;
import tech.ydb.yoj.repository.ydb.TestYdbRepository;
import tech.ydb.yoj.repository.ydb.YdbEnvAndTransportRule;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

public class YqlQueryMergerIntegrationTest extends RepositoryTestSupport {
    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    protected ScopedTxManager<TestDb> tx;

    @Override
    public void setUp() {
        super.setUp();
        this.tx = new ScopedTxManager<>(this.repository, TestDb.class);
    }

    @Override
    public void tearDown() {
        this.tx = null;
        super.tearDown();
    }

    @Override
    protected final Repository createRepository() {
        return TestEntities.init(TestYdbRepository.create(ydbEnvAndTransport));
    }

    @Test
    public void insertAfterFindByIdWorks() {
        tx.run(db -> {
            Project.Id id = new Project.Id(RandomUtils.nextString(10));

            assertThat(db.projects().find(id)).isNull();

            db.projects().insert(new Project(id, "project-1"));
        });
    }

    private static final class RandomUtils {
        private static final String alphanum = createAlphabet();

        private static String createAlphabet() {
            String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            return upper + upper.toLowerCase(Locale.ROOT) + "0123456789";
        }

        public static String nextString(int length) {
            char[] buf = new char[length];
            for (int idx = 0; idx < buf.length; ++idx) {
                buf[idx] = alphanum.charAt(ThreadLocalRandom.current().nextInt(alphanum.length()));
            }
            return new String(buf);
        }
    }
}
