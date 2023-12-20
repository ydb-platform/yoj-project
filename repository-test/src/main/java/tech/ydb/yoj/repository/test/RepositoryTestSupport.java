package tech.ydb.yoj.repository.test;

import org.junit.After;
import org.junit.Before;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.StdTxManager;
import tech.ydb.yoj.repository.db.Tx;

import java.util.IdentityHashMap;
import java.util.Map;

public abstract class RepositoryTestSupport {
    private static final Map<Class<?>, Repository> repositoryMap = new IdentityHashMap<>();

    protected Repository repository;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> repositoryMap.values().forEach(Repository::dropDb)));
    }

    protected abstract Repository createRepository();

    @Before
    public void setUp() {
        this.repository = repositoryMap.computeIfAbsent(this.getClass(), aClass -> createRepository());
    }

    @After
    public void tearDown() {
        clearDb(this.repository);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void clearDb(Repository repo) {
        var tables = repo.tables();
        new StdTxManager(repo).tx(() -> tables
                .forEach(table -> Tx.Current.get().getRepositoryTransaction().table((Class) table).deleteAll()));
    }
}
