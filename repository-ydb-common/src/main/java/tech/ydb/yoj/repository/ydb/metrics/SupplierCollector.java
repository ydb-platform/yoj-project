package tech.ydb.yoj.repository.ydb.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.InternalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

@InternalApi
public class SupplierCollector extends SimpleCollector<SupplierCollector.Child> implements Collector.Describable {
    private static final Logger log = LoggerFactory.getLogger(SupplierCollector.class);

    private final Collector.Type type;

    private SupplierCollector(Builder builder) {
        super(builder);
        this.type = builder.type;
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
        return familySamplesList(type, samples);
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

    public static class Builder extends SimpleCollector.Builder<Builder, SupplierCollector> {
        private Collector.Type type = Collector.Type.GAUGE;

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        @Override
        public SupplierCollector create() {
            return new SupplierCollector(this);
        }
    }

    public class Child {
        private volatile Supplier<? extends Number> supplier = () -> 0.;

        public SupplierCollector supplier(Supplier<? extends Number> supplier) {
            this.supplier = Objects.requireNonNull(supplier, "supplier");
            return SupplierCollector.this;
        }

        public double getValue() {
            return supplier.get().doubleValue();
        }
    }
}
