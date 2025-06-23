package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import lombok.SneakyThrows;
import tech.ydb.test.integration.YdbHelper;
import tech.ydb.test.junit4.GrpcTransportRule;
import tech.ydb.yoj.repository.ydb.client.YdbPaths;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * YDB configuration for integration tests.
 */
public class TestYdbConfig {
    @Deprecated(forRemoval = true)
    private static final YdbConfig config = create(String.valueOf(Instant.now().toEpochMilli()));

    public static YdbConfig create(YdbHelper helper, Instant started) {
        return create(helper, String.valueOf(started.toEpochMilli()));
    }

    private static YdbConfig create(YdbHelper helper, String repId) {
        String endpoint = helper.endpoint();
        HostAndPort hostAndPort = HostAndPort.fromString(endpoint);
        return YdbConfig
                .createForTesting(
                        hostAndPort.getHost(),
                        hostAndPort.getPort(),
                        YdbPaths.join(helper.database(), "it-" + repId + "/"),
                        helper.database()
                )
                .withSessionKeepAliveTime(Duration.ofSeconds(10))
                .withSessionMaxIdleTime(Duration.ofSeconds(10))
                .withUseTLS(helper.useTls())
                .withRootCA(helper.pemCert())
                .withUseTrustStore(helper.pemCert() == null)
                ;
    }

    /**
     * @deprecated This methods returns an environment variable-based {@code YdbConfig} for integration tests with YDB that's
     * already running locally (either in a Docker container, or as a standalone service).
     * <br>
     * We've transitioned to TestContainers, please use {@link GrpcTransportRule} and {@link #create(YdbHelper, Instant)} instead.
     */
    @Deprecated(forRemoval = true)
    public static YdbConfig get() {
        return config;
    }

    @Deprecated(forRemoval = true)
    private static YdbConfig create(String repId) {
        return YdbConfig
                .createForTesting(
                        getStr("YDB_HOST", "localhost"),
                        getInt("YDB_PORT", 2135),
                        getStr("YDB_TABLESPACE", "/local/ycloud/it-" + repId + "/"),
                        getStr("YDB_DATABASE", "/local")
                )
                .withSessionKeepAliveTime(Duration.ofSeconds(10))
                .withSessionMaxIdleTime(Duration.ofSeconds(10))
                .withUseTLS(Boolean.parseBoolean(getStr("YDB_USE_SSL", "false")))
                .withRootCA(getFile("YDB_ROOT_CA", null))
                ;
    }

    private static String getStr(String name, String defaultValue) {
        String value = System.getenv(name);
        return !Strings.isNullOrEmpty(value) ? value : defaultValue;
    }

    private static int getInt(String name, int defaultValue) {
        String value = System.getProperty(name);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @SneakyThrows
    private static byte[] getFile(String name, byte[] defaultValue) {
        String fileName = getStr(name, null);
        return Strings.isNullOrEmpty(fileName) ? defaultValue : Files.readAllBytes(Paths.get(fileName));
    }
}
