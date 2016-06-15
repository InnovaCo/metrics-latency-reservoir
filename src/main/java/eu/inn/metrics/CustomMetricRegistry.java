package eu.inn.metrics;


import com.codahale.metrics.*;

import java.util.concurrent.ConcurrentMap;

public class CustomMetricRegistry extends MetricRegistry {

    private ConcurrentMap<String, Metric> metrics;

    private final MetricBuilder<Timer> timersBuilder;

    private final MetricBuilder<Histogram> histogramsBuilder;

    private final MetricBuilder<Meter> metersBuilder;

    private final MetricBuilder<Counter> countersBuilder;

    CustomMetricRegistry(MetricBuilder<Timer> timersBuilder, MetricBuilder<Histogram> histogramsBuilder, MetricBuilder<Meter> metersBuilder, MetricBuilder<Counter> countersBuilder) {
        this.timersBuilder = timersBuilder;
        this.histogramsBuilder = histogramsBuilder;
        this.metersBuilder = metersBuilder;
        this.countersBuilder = countersBuilder;
    }

    @Override
    public Timer timer(String name) {
        return getOrAdd(name, timersBuilder);
    }

    @Override
    public Histogram histogram(String name) {
        return getOrAdd(name, histogramsBuilder);
    }

    @Override
    public Meter meter(String name) {
        return getOrAdd(name, metersBuilder);
    }

    @Override
    public Counter counter(String name) {
        return getOrAdd(name, countersBuilder);
    }

    @Override
    protected ConcurrentMap<String, Metric> buildMap() {
        metrics = super.buildMap();
        return metrics;
    }

    protected  <T extends Metric> T getOrAdd(String name, MetricBuilder<T> builder) {
        final Metric metric = metrics.get(name);
        if (builder.isInstance(metric)) {
            return (T) metric;
        } else if (metric == null) {
            try {
                return register(name, builder.newMetric());
            } catch (IllegalArgumentException e) {
                final Metric added = metrics.get(name);
                if (builder.isInstance(added)) {
                    return (T) added;
                }
            }
        }
        throw new IllegalArgumentException(name + " is already used for a different type of metric");
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private MetricBuilder<Timer> timersBuilder = DefaultMetricBuilder.TIMERS;
        private MetricBuilder<Histogram> histogramsBuilder = DefaultMetricBuilder.HISTOGRAMS;
        private MetricBuilder<Meter> metersBuilder = DefaultMetricBuilder.METERS;
        private MetricBuilder<Counter> countersBuilder = DefaultMetricBuilder.COUNTERS;

        public Builder timersBuilder(MetricBuilder<Timer> timersBuilder) {
            this.timersBuilder = timersBuilder;
            return this;
        }

        public Builder histogramsBuilder(MetricBuilder<Histogram> histogramsBuilder) {
            this.histogramsBuilder = histogramsBuilder;
            return this;
        }

        public Builder metersBuilder(MetricBuilder<Meter> metersBuilder) {
            this.metersBuilder = metersBuilder;
            return this;
        }

        public Builder countersBuilder(MetricBuilder<Counter> countersBuilder) {
            this.countersBuilder = countersBuilder;
            return this;
        }

        public CustomMetricRegistry build() {
            return new CustomMetricRegistry(timersBuilder, histogramsBuilder, metersBuilder, countersBuilder);
        }
    }
}
