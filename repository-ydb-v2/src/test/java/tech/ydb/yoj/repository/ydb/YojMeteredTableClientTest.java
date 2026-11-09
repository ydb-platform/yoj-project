package tech.ydb.yoj.repository.ydb;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.SessionPoolStats;
import tech.ydb.table.TableClient;
import tech.ydb.yoj.tracer.YojTracer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class YojMeteredTableClientTest {
    private final TableClient delegate = mock(TableClient.class);
    private final YojTracer tracer = mock(YojTracer.class);
    private final YojTracer.SessionPool pool = mock(YojTracer.SessionPool.class);

    @Test
    public void suppliesSdkIndependentStatsAndUnregistersBeforeClosingDelegate() {
        var stats = mock(SessionPoolStats.class);
        when(stats.getMinSize()).thenReturn(1);
        when(stats.getMaxSize()).thenReturn(2);
        when(stats.getIdleCount()).thenReturn(3);
        when(stats.getAcquiredCount()).thenReturn(4);
        when(stats.getPendingAcquireCount()).thenReturn(5);
        when(stats.getAcquiredTotal()).thenReturn(6L);
        when(stats.getReleasedTotal()).thenReturn(7L);
        when(stats.getRequestedTotal()).thenReturn(8L);
        when(stats.getCreatedTotal()).thenReturn(9L);
        when(stats.getFailedTotal()).thenReturn(10L);
        when(stats.getDeletedTotal()).thenReturn(11L);
        when(delegate.sessionPoolStats()).thenReturn(stats);

        TableClient client = newClient();
        ArgumentCaptor<Supplier<YojTracer.SessionPoolStats>> supplier = ArgumentCaptor.captor();
        verify(tracer).registerSessionPool(eq("orders"), supplier.capture());
        assertThat(supplier.getValue().get()).isEqualTo(new YojTracer.SessionPoolStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11));
        client.close();
        client.close();

        var order = inOrder(pool, delegate);
        order.verify(pool).close();
        order.verify(delegate).close();
        verify(pool, times(1)).close();
        verify(delegate, times(1)).close();
    }

    @Test
    public void closesEachAcquisitionTimerEvenWhenCompletionFollowsShutdown() {
        var firstTimer = mock(YojTracer.Timer.class);
        var pendingTimer = mock(YojTracer.Timer.class);
        when(pool.startAcquireTimer()).thenReturn(firstTimer).thenReturn(pendingTimer);
        var first = new CompletableFuture<Result<Session>>();
        var pending = new CompletableFuture<Result<Session>>();
        when(delegate.createSession(any())).thenReturn(first).thenReturn(pending);

        TableClient client = newClient();
        var result = Result.success(mock(Session.class));
        var firstAcquisition = client.createSession(Duration.ofSeconds(1));
        first.complete(result);
        assertThat(firstAcquisition.join()).isSameAs(result);
        client.createSession(Duration.ofSeconds(1));
        client.close();
        pending.complete(result);

        verify(pool, times(2)).startAcquireTimer();
        verify(firstTimer).close();
        verify(pendingTimer).close();
    }

    @Test
    public void cancellationReachesSdkFutureAndClosesAcquisitionTimer() {
        var timer = mock(YojTracer.Timer.class);
        when(pool.startAcquireTimer()).thenReturn(timer);
        var pending = new CompletableFuture<Result<Session>>();
        when(delegate.createSession(any())).thenReturn(pending);

        try (TableClient client = newClient()) {
            var acquisition = client.createSession(Duration.ofSeconds(1));
            assertThat(acquisition.cancel(false)).isTrue();
            assertThat(pending).isCancelled();
            verify(timer).close();
        }
    }

    private TableClient newClient() {
        var transport = mock(GrpcTransport.class);
        var builder = mock(TableClient.Builder.class, RETURNS_SELF);
        when(builder.build()).thenReturn(delegate);
        when(tracer.registerSessionPool(eq("orders"), any())).thenReturn(pool);
        var settings = YdbRepository.Settings.builder()
                .metrics(new YdbRepository.Settings.Metrics("orders", tracer))
                .build();
        try (var tableClient = mockStatic(TableClient.class)) {
            tableClient.when(() -> TableClient.newClient(transport)).thenReturn(builder);
            return YojMeteredTableClient.newClient(YdbConfig.createForTesting("localhost", 2135, "test", "/test"),
                    settings, transport);
        }
    }
}
