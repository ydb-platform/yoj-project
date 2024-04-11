package tech.ydb.yoj.repository.ydb;

import io.grpc.MethodDescriptor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.core.grpc.GrpcReadWriteStream;
import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.yoj.util.function.MoreSuppliers;
import tech.ydb.yoj.util.function.MoreSuppliers.CloseableMemoizer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Lazily constructed YDB SDK {@link GrpcTransport}, to defer testing the YDB connection to actual first usage of {@link YdbRepository},
 * instead of the {@link YdbRepository} constructor.
 */
@RequiredArgsConstructor
/*package*/ final class LazyGrpcTransport implements GrpcTransport {
    @Getter
    private final String database;

    private final CloseableMemoizer<GrpcTransport> transport;

    /*package*/ LazyGrpcTransport(GrpcTransportBuilder builder, Function<GrpcTransportBuilder, GrpcTransport> init) {
        this.database = builder.getDatabase();
        this.transport = MoreSuppliers.memoizeCloseable(() -> init.apply(builder));
    }

    @Override
    public <ReqT, RespT> CompletableFuture<Result<RespT>> unaryCall(MethodDescriptor<ReqT, RespT> method, GrpcRequestSettings settings, ReqT request) {
        return transport.get().unaryCall(method, settings, request);
    }

    @Override
    public <ReqT, RespT> GrpcReadStream<RespT> readStreamCall(MethodDescriptor<ReqT, RespT> method, GrpcRequestSettings settings, ReqT request) {
        return transport.get().readStreamCall(method, settings, request);
    }

    @Override
    public <ReqT, RespT> GrpcReadWriteStream<RespT, ReqT> readWriteStreamCall(MethodDescriptor<ReqT, RespT> method, GrpcRequestSettings settings) {
        return transport.get().readWriteStreamCall(method, settings);
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return transport.get().getScheduler();
    }

    @Override
    public void close() {
        transport.close();
    }

    @Override
    public String toString() {
        return "LazyGrpcTransport[transport created: " + transport.isInitialized() + "]";
    }
}
