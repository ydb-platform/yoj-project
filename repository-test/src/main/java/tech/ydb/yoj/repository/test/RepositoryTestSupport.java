package tech.ydb.yoj.repository.test;

import org.junit.After;
import org.junit.Before;
import tech.ydb.yoj.databind.schema.reflect.StdReflector;
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
        StdReflector.enableStrictMode();
        this.repository = repositoryMap.computeIfAbsent(this.getClass(), aClass -> createRepository());
    }

    @After
    public void tearDown() {
        StdReflector.disableStrictMode();
        clearDb(this.repository);
    }

    public static void clearDb(Repository repo) {
        Set<TableDescriptor<?>> tableDescriptors = repo.tables();
        new StdTxManager(repo).tx(() -> {
            for (TableDescriptor<?> tableDescriptor : tableDescriptors) {
                Tx.Current.get().getRepositoryTransaction().table(tableDescriptor).deleteAll();
            }
        });
    }
}
