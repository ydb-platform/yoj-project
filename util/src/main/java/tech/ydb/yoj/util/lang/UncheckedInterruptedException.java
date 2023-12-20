package tech.ydb.yoj.util.lang;

public class UncheckedInterruptedException extends RuntimeException {
    public UncheckedInterruptedException(InterruptedException e) {
        super(e);
    }
}
