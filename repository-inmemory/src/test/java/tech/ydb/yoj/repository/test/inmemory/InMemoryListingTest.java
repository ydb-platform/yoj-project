package tech.ydb.yoj.repository.test.inmemory;

import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.test.ListingTest;

public class InMemoryListingTest extends ListingTest {
    @Override
    protected Repository createTestRepository() {
        return new TestInMemoryRepository();
    }
}
