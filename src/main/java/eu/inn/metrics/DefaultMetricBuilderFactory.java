package eu.inn.metrics;

import com.codahale.metrics.*;
import eu.inn.metrics.sed.SlidingExponentialDecayingReservoir;
import eu.inn.metrics.staff.TimeWindowReservoirBuilder;

import java.util.concurrent.TimeUnit;

public class DefaultMetricBuilderFactory implements MetricBuilderFactory {

    private final static TimeWindowReservoirBuilder defaultReservoirBuilder = SlidingExponentialDecayingReservoir.builder()
            .flushAt(1, TimeUnit.SECONDS)
            .window(15, TimeUnit.SECONDS);

    private static final MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
        @Override
        public Histogram newMetric() {
            return new Histogram(defaultReservoirBuilder.build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Histogram.class.isInstance(metric);
        }
    };


    private static final MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
        @Override
        public Timer newMetric() {
            return new Timer(defaultReservoirBuilder.build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Timer.class.isInstance(metric);
        }
    };

    private static final MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
        @Override
        public Counter newMetric() {
            return new Counter();
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Counter.class.isInstance(metric);
        }
    };

    private static final MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
        @Override
        public Meter newMetric() {
            return new Meter();
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Meter.class.isInstance(metric);
        }
    };

    @Override
    public MetricBuilder<Histogram> histogramsBuilder() {
        return HISTOGRAMS;
    }

    @Override
    public MetricBuilder<Timer> timersBuilder() {
        return TIMERS;
    }

    @Override
    public MetricBuilder<Counter> countersBuilder() {
        return COUNTERS;
    }

    @Override
    public MetricBuilder<Meter> metersBuilder() {
        return METERS;
    }
}