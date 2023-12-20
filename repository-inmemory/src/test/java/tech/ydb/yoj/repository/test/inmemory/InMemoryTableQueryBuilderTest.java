package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.TableQueryBuilderTest;

public class InMemoryTableQueryBuilderTest extends TableQueryBuilderTest {
    @Override
    protected Repository createTestRepository() {
        return new TestInMemoryRepository();
    }
}
