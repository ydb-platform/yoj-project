package tech.ydb.yoj.repository.ydb.query;

import org.junit.ClassRule;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.TableQueryBuilderTest;
import tech.ydb.yoj.repository.ydb.TestYdbRepository;
import tech.ydb.yoj.repository.ydb.YdbEnvAndTransportRule;

public class YdbTableQueryBuilderIntegrationTest extends TableQueryBuilderTest {
    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    @Override
    protected Repository createTestRepository() {
        return new TestYdbRepository(ydbEnvAndTransport.getYdbConfig(), ydbEnvAndTransport.getGrpcTransport());
    }
}
