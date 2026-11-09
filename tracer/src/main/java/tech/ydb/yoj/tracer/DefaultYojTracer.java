package tech.ydb.yoj.tracer;

import java.util.ServiceLoader;

final class DefaultYojTracer {
    static final YojTracer INSTANCE = ServiceLoader.load(YojTracer.Provider.class, YojTracer.class.getClassLoader())
            .findFirst()
            .map(YojTracer.Provider::getTracer)
            .orElse(YojTracer.NONE);

    private DefaultYojTracer() {
    }
}
