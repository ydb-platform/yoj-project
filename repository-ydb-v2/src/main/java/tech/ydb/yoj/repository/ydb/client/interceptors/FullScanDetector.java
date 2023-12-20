package tech.ydb.yoj.repository.ydb.client.interceptors;

import tech.ydb.table.Session;
import tech.ydb.yoj.repository.ydb.client.QueryInterceptingSession.QueryType;
import tech.ydb.yoj.repository.ydb.client.QueryInterceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static tech.ydb.yoj.repository.ydb.client.QueryInterceptingSession.QueryType.DATA_QUERY;
import static tech.ydb.yoj.repository.ydb.client.QueryInterceptingSession.QueryType.SCAN_QUERY;

public final class FullScanDetector implements QueryInterceptor {
    private static final Map<String, Boolean> executedQueries = new ConcurrentHashMap<>();
    private final Set<String> ignoredQueries;
    private final Map<QueryType, List<Consumer<String>>> callbacks;

    private FullScanDetector(Set<String> ignoredQueries, Map<QueryType, List<Consumer<String>>> callbacks) {
        this.ignoredQueries = ignoredQueries;
        this.callbacks = callbacks;
    }

    public static FullScanDetectorBuilder builder() {
        return new FullScanDetectorBuilder();
    }

    private static final ThreadLocal<Boolean> ignoreFullScan = ThreadLocal.withInitial(() -> false);

    public static <T> T ignoringFullScan(Supplier<T> supplier) {
        ignoreFullScan.set(true);
        try {
            return supplier.get();
        } finally {
            ignoreFullScan.set(false);
        }
    }

    public static void ignoringFullScan(Runnable runnable) {
        ignoringFullScan(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public void beforeExecute(QueryType type, Session session, String query) {
        if (mustHandle(session, query)) {
            for (Consumer<String> callback : callbacks.get(type)) {
                callback.accept(query);
            }
        }
    }

    private boolean mustHandle(Session session, String query) {
        if (ignoreFullScan.get()) {
            return false;
        }
        if (ignoredQueries.contains(query)) {
            return false;
        }
        if (executedQueries.containsKey(query)) {
            return false;
        }
        String plan = explain(session, query);
        executedQueries.put(query, true);
        return plan.contains("FullScan");
    }

    private String explain(Session session, String query) {
        return session.explainDataQuery(query).join().getValue().getQueryPlan();
    }


    public static class FullScanDetectorBuilder {
        private Set<String> ignoredQueries = Collections.emptySet();
        private final List<Consumer<String>> scanQueryFullScanCallbacks = new ArrayList<>();
        private final List<Consumer<String>> dataQueryFullScanCallbacks = new ArrayList<>();

        FullScanDetectorBuilder() {
        }

        public FullScanDetectorBuilder ignoredQueries(Collection<String> ignoredQueries) {
            this.ignoredQueries = Set.copyOf(ignoredQueries);
            return this;
        }

        public FullScanDetectorBuilder callback(QueryType type, Consumer<String> callback) {
            switch (type) {
                case SCAN_QUERY -> scanQueryFullScanCallbacks.add(callback);
                case DATA_QUERY -> dataQueryFullScanCallbacks.add(callback);
                default -> throw new IllegalArgumentException();
            }
            return this;
        }

        public FullScanDetectorBuilder callback(Consumer<String> callback) {
            scanQueryFullScanCallbacks.add(callback);
            dataQueryFullScanCallbacks.add(callback);
            return this;
        }

        public FullScanDetector build() {
            return new FullScanDetector(ignoredQueries, Map.of(
                    SCAN_QUERY, scanQueryFullScanCallbacks,
                    DATA_QUERY, dataQueryFullScanCallbacks
            ));
        }
    }
}
