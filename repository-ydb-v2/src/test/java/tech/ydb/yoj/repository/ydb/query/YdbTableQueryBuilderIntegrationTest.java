package tech.ydb.yoj.repository.ydb.query;

import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.TableQueryBuilderTest;
import tech.ydb.yoj.repository.ydb.TestYdbConfig;
import tech.ydb.yoj.repository.ydb.TestYdbRepository;

public class YdbTableQueryBuilderIntegrationTest extends TableQueryBuilderTest {
    @Override
    protected Repository createTestRepository() {
        return new TestYdbRepository(TestYdbConfig.get());
    }
}
