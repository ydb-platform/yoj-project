package tech.ydb.yoj.repository.test;

import org.junit.After;
import org.junit.Before;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.db.Tx;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

public abstract class RepositoryTestSupport {
    private static final Map<Class<?>, Repository> repositoryMap = new IdentityHashMap<>();

    protected Repository repository;

    protected abstract Repository createRepository();

    @Before
    public void setUp() {
        this.repository = repositoryMap.computeIfAbsent(this.getClass(), aClass -> createRepository());
    }

    @After
    public void tearDown() {
        this.repository.getSchemaOperations().removeTablespace();
    }
}
