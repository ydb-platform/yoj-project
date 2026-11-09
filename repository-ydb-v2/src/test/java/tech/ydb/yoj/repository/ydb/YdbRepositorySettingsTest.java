package tech.ydb.yoj.repository.ydb;

import org.junit.Test;
import tech.ydb.yoj.tracer.YojTracer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

public class YdbRepositorySettingsTest {
    @Test
    public void defaultSettingsUseDiscoveredTracer() {
        YojTracer tracer = YojTracer.getDefault();
        assertThat(tracer).isNotSameAs(YojTracer.NONE);
        assertThat(YdbRepository.Settings.builder().build().metrics().tracer()).isSameAs(tracer);
        assertThat(YdbRepository.Settings.Metrics.builder().build().tracer()).isSameAs(tracer);
        assertThat(new YdbRepository.Settings.Metrics("orders").tracer()).isSameAs(tracer);
    }

    @Test
    public void preservesExplicitCustomAndNoOpTracers() {
        try (var defaultTracer = mockStatic(YojTracer.class)) {
            for (YojTracer tracer : new YojTracer[]{new YojTracer() { }, YojTracer.NONE}) {
                var metrics = YdbRepository.Settings.Metrics.builder().tracer(tracer).build();
                var settings = YdbRepository.Settings.builder().metrics(metrics).build();
                assertThat(settings.metrics().tracer()).isSameAs(tracer);
                assertThat(new YdbRepository.Settings.Metrics("orders", tracer).tracer()).isSameAs(tracer);
            }
            defaultTracer.verifyNoInteractions();
        }
    }
}
