package tech.ydb.yoj.repository.ydb;

import com.google.common.net.HostAndPort;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.With;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Value
@Builder
public class YdbConfig {
    private static final Duration SESSION_KEEP_ALIVE_TIME_DEFAULT = Duration.ofMinutes(5);
    private static final Duration SESSION_MAX_IDLE_TIME_DEFAULT = Duration.ofMinutes(5);
    private static final Duration TCP_KEEP_ALIVE_TIME_DEFAULT = Duration.ofSeconds(5);
    private static final Duration TCP_KEEP_ALIVE_TIMEOUT_DEFAULT = Duration.ofSeconds(1);
    private static final Duration SESSION_CREATE_TIMEOUT_DEFAULT = Duration.ofSeconds(1);
    private static final int SESSION_POOL_SIZE_DEFAULT = 100;
    private static final int SESSION_CREATE_RETRY_COUNT_DEFAULT = 3;

    public static YdbConfig createForTesting(String host, int port, String tablespace, String database) {
        return new YdbConfig(
                tablespace,
                database,
                null,
                List.of(HostAndPort.fromParts(host, port)),
                SESSION_CREATE_TIMEOUT_DEFAULT,
                SESSION_CREATE_RETRY_COUNT_DEFAULT,
                SESSION_KEEP_ALIVE_TIME_DEFAULT,
                SESSION_MAX_IDLE_TIME_DEFAULT,
                SESSION_POOL_SIZE_DEFAULT,
                SESSION_POOL_SIZE_DEFAULT,
                TCP_KEEP_ALIVE_TIME_DEFAULT,
                TCP_KEEP_ALIVE_TIMEOUT_DEFAULT,
                false,
                false,
                null
        );
    }

    /**
     * Base path for all YDB tables.
     */
    @NonNull
    @With
    String tablespace;

    /**
     * Path to the YDB database.
     */
    @NonNull
    @With
    String database;

    // oneof {discoveryEndpoint, endpoints}
    @With
    String discoveryEndpoint;

    /**
     * In sdk-v2 only one endpoint is possible.
     * Please use only {@link YdbConfig#discoveryEndpoint} in config.
     */
    @Deprecated(forRemoval = true)
    List<HostAndPort> endpoints;

    @With
    Duration sessionCreationTimeout;
    @With
    Integer sessionCreationMaxRetries;
    @With
    Duration sessionKeepAliveTime;
    @With
    Duration sessionMaxIdleTime;
    @With
    Integer sessionPoolMin;
    @With
    Integer sessionPoolMax;

    @With
    Duration tcpKeepaliveTime;
    @With
    Duration tcpKeepaliveTimeout;

    @With
    boolean useTLS;

    // oneof {useTrustStore, rootCA}
    /**
     * Use RootCA certificates from JDK TrustStore.
     */
    @With
    boolean useTrustStore;
    /**
     * RootCA certificate content. (Will be ignored if {@link #useTrustStore} is {@code true})
     */
    @With
    byte[] rootCA;

    public Duration getSessionCreationTimeout() {
        return Optional.ofNullable(sessionCreationTimeout).orElse(SESSION_CREATE_TIMEOUT_DEFAULT);
    }

    public Integer getSessionCreationMaxRetries() {
        return Optional.ofNullable(sessionCreationMaxRetries).orElse(SESSION_CREATE_RETRY_COUNT_DEFAULT);
    }

    public Duration getSessionKeepAliveTime() {
        return Optional.ofNullable(sessionKeepAliveTime).orElse(SESSION_KEEP_ALIVE_TIME_DEFAULT);
    }

    public Duration getSessionMaxIdleTime() {
        return Optional.ofNullable(sessionMaxIdleTime).orElse(SESSION_MAX_IDLE_TIME_DEFAULT);
    }

    public Integer getSessionPoolMin() {
        return Optional.ofNullable(sessionPoolMin).orElse(SESSION_POOL_SIZE_DEFAULT);
    }

    public Integer getSessionPoolMax() {
        return Optional.ofNullable(sessionPoolMax).orElse(SESSION_POOL_SIZE_DEFAULT);
    }

    public Duration getTcpKeepaliveTime() {
        return Optional.ofNullable(tcpKeepaliveTime).orElse(TCP_KEEP_ALIVE_TIME_DEFAULT);
    }

    public Duration getTcpKeepaliveTimeout() {
        return Optional.ofNullable(tcpKeepaliveTimeout).orElse(TCP_KEEP_ALIVE_TIMEOUT_DEFAULT);
    }
}
