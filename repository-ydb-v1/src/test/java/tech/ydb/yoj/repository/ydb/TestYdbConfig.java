package tech.ydb.yoj.repository.ydb;

import com.google.common.base.Strings;
import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * YDB configuration for integration tests.
 */
public class TestYdbConfig {
    private static final YdbConfig config = create(String.valueOf(Instant.now().toEpochMilli()));

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

    public static YdbConfig get() {
        return config;
    }

    public static YdbConfig create(String repId) {
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
}
