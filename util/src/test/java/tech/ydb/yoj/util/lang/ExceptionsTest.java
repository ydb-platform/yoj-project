package tech.ydb.yoj.util.lang;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class ExceptionsTest {
    @Test
    public void normalCause() {
        RuntimeException e = new RuntimeException(new InterruptedException("yay, interrupts!"));
        assertThat(Exceptions.isOrCausedBy(e, InterruptedException.class)).isTrue();
    }

    @Test
    public void deepCause() {
        RuntimeException e = new RuntimeException(new ExecutionException(new InterruptedException("yay, interrupts!")));
        assertThat(Exceptions.isOrCausedBy(e, InterruptedException.class)).isTrue();
    }

    @Test
    public void self() {
        InterruptedException e = new InterruptedException("yay, interrupts!");
        assertThat(Exceptions.isOrCausedBy(e, InterruptedException.class)).isTrue();
    }

    @Test
    public void superclassCause() {
        IllegalArgumentException nfe = new NumberFormatException();
        RuntimeException b = new RuntimeException(nfe);

        assertThat(Exceptions.isOrCausedBy(b, IllegalArgumentException.class)).isTrue();
    }

    @Test
    public void noCause() {
        Exception e = new Exception("no interrupts");
        assertThat(Exceptions.isOrCausedBy(e, InterruptedException.class)).isFalse();
    }

    @Test
    public void causalLoop() {
        IllegalArgumentException a = new IllegalArgumentException();
        RuntimeException b = new RuntimeException(a);
        a.initCause(b);

        assertThat(Exceptions.isOrCausedBy(b, UnsupportedOperationException.class)).isFalse();
    }
}

