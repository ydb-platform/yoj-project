package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.RepositoryTest;

public class InMemoryRepositoryTest extends RepositoryTest {
    @Override
    protected Repository createTestRepository() {
        return new TestInMemoryRepository();
    }
}
