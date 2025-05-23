package tech.ydb.yoj.repository.ydb;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.test.integration.YdbHelper;
import tech.ydb.test.integration.YdbHelperFactory;
import tech.ydb.test.integration.utils.ProxyGrpcTransport;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariableMocker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public final class YdbEnvAndTransportRule implements TestRule {
    private final Instant started = Instant.now();
    private final GrpcTransportRule transportRule = new GrpcTransportRule();

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                List<Throwable> errors = new ArrayList<>();
                try {
                    Map<String, String> evm = new HashMap<>();
                    evm.put("YDB_DOCKER_IMAGE", "docker.io/ydbplatform/local-ydb:24.4.4");

                    Statement ydbEnrichedStatement;

                    EnvironmentVariableMocker.connect(evm, Set.of());
                    try {
                        ydbEnrichedStatement = transportRule.apply(base, description);
                    } finally {
                        EnvironmentVariableMocker.remove(evm);
                    }

                    ydbEnrichedStatement.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        transportRule.close();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    public GrpcTransport getGrpcTransport() {
        return transportRule;
    }

    public YdbConfig getYdbConfig() {
        return TestYdbConfig.create(transportRule.helper(), started);
    }

    private static final class GrpcTransportRule extends ProxyGrpcTransport implements TestRule {
        private static final Logger logger = LoggerFactory.getLogger(GrpcTransportRule.class);

        private final AtomicReference<YdbHelper> helper = new AtomicReference<>();
        private final AtomicReference<GrpcTransport> transport = new AtomicReference<>();

        @Override
        public Statement apply(Statement base, Description description) {
            YdbHelperFactory factory = YdbHelperFactory.getInstance();

            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    if (!factory.isEnabled()) {
                        logger.info("Test {} skipped because ydb helper is not available", description.getDisplayName());
                        Assume.assumeFalse("YDB Helper is not available", true);
                        return;
                    }

                    String path = description.getClassName();
                    if (description.getMethodName() != null) {
                        path += "/" + description.getMethodName();
                    }

                    logger.debug("create ydb helper for test {}", path);
                    try (YdbHelper helper = factory.createHelper()) {
                        GrpcTransportRule.this.helper.set(helper);
                        try (GrpcTransport transport = helper.createTransport()) {
                            GrpcTransportRule.this.transport.set(transport);
                            base.evaluate();
                        } finally {
                            GrpcTransportRule.this.transport.set(null);
                        }
                    } finally {
                        GrpcTransportRule.this.helper.set(null);
                    }
                }
            };
        }

        @Override
        protected GrpcTransport origin() {
            return GrpcTransportRule.this.transport.get();
        }

        protected YdbHelper helper() {
            return GrpcTransportRule.this.helper.get();
        }
    }
}
