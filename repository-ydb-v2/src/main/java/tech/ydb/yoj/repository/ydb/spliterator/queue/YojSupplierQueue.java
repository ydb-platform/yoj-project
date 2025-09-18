package tech.ydb.yoj.repository.ydb.spliterator.queue;

public interface YojSupplierQueue<V> {
    // (grpc thread) Send values to user-stream.
    boolean offer(V value) throws OfferDeadlineExceededException;

    // (grpc thread) Send knowledge to user-stream when data is over (or error handled).
    void supplierDone(Runnable status);

    boolean isClosed();
}
