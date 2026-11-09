package tech.ydb.yoj.tracer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class YojTracerTest {
    @Test
    public void defaultsToNoOpWithoutProvider() {
        assertThat(YojTracer.getDefault()).isSameAs(YojTracer.NONE);
    }
}
