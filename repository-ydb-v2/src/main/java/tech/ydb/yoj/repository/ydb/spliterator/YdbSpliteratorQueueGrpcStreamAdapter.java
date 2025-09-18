package tech.ydb.yoj.repository.ydb.spliterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.Status;
import tech.ydb.yoj.repository.ydb.YdbOperations;
import tech.ydb.yoj.repository.ydb.spliterator.queue.OfferDeadlineExceededException;
import tech.ydb.yoj.repository.ydb.spliterator.queue.YojQueue;
import tech.ydb.yoj.repository.ydb.spliterator.queue.YojSupplierQueue;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static tech.ydb.yoj.repository.ydb.client.YdbValidator.validate;

public final class YdbSpliteratorQueueGrpcStreamAdapter<V> {
    private static final Logger log = LoggerFactory.getLogger(YdbSpliteratorQueueGrpcStreamAdapter.class);

    private final String request;
    private final YojSupplierQueue<V> queue;

    public YdbSpliteratorQueueGrpcStreamAdapter(String request, YojQueue<V> queue) {
        this.request = request;
        this.queue = queue;
    }

    public void onNext(V values) {
        if (!queue.offer(values)) {
            // Need to abort supplier thread if stream is closed. onSupplierThreadComplete will exit immediately.
            // ConsumerDoneException isn't handled because onSupplierThreadComplete will exit by this.closed.
            throw ConsumerDoneException.INSTANCE;
        }
    }

    // (supplier thread) Send knowledge to stream when data is over.
    public void onSupplierThreadComplete(Status status, Throwable ex) {
        var error = unwrapException(ex);
        if (queue.isClosed() || error instanceof OfferDeadlineExceededException) {
            log.error("Supplier thread was closed because consumer didn't poll an element of stream on timeout");
            // If deadline exceeded happen, need to do nothing. Stream thread will exit at deadline by themself.
            return;
        }

        queue.supplierDone(() -> {
            if (error != null) {
                throw YdbOperations.convertToRepositoryException(error);
            }

            validate(request, status.getCode(), status.toString());
        });
    }

    private static Throwable unwrapException(Throwable ex) {
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            return ex.getCause();
        }
        return ex;
    }

    private static class ConsumerDoneException extends RuntimeException {
        public final static ConsumerDoneException INSTANCE = new ConsumerDoneException();
    }
}
