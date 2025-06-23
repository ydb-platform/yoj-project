package tech.ydb.yoj.repository.ydb.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.InternalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@InternalApi
public class GaugeSupplierCollector extends SimpleCollector<GaugeSupplierCollector.Child> implements Collector.Describable {
    private static final Logger log = LoggerFactory.getLogger(GaugeSupplierCollector.class);

    private GaugeSupplierCollector(Builder builder) {
        super(builder);
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<>();
        children.forEach((labelValues, child) -> {
            try {
                samples.add(new MetricFamilySamples.Sample(fullname, labelNames, labelValues, child.getValue()));
            } catch (Exception e) {
                log.error("Could not add child sample", e);
            }
        });
        return familySamplesList(Type.GAUGE, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return List.of(new GaugeMetricFamily(fullname, help, labelNames));
    }

    @Override
    protected Child newChild() {
        return new Child();
    }

    public void supplier(Supplier<Number> supplier) {
        this.noLabelsChild.supplier(supplier);
    }

    @Accessors(fluent = true)
    public static class Builder extends SimpleCollector.Builder<Builder, GaugeSupplierCollector> {
        @Override
        public GaugeSupplierCollector create() {
            return new GaugeSupplierCollector(this);
        }
    }

    public class Child {
        private Supplier<? extends Number> supplier = () -> 0.;

        public GaugeSupplierCollector supplier(Supplier<? extends Number> supplier) {
            this.supplier = supplier;
            return GaugeSupplierCollector.this;
        }

        public double getValue() {
            return supplier.get().doubleValue();
        }
    }
}
