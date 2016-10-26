package eu.inn.metrics;

import com.codahale.metrics.*;
import eu.inn.metrics.sed.SlidingExponentialDecayingReservoir;
import eu.inn.metrics.common.TimeWindowReservoirBuilder;

import java.util.concurrent.TimeUnit;

public class TimeWindowMetricBuilderFactory implements MetricBuilderFactory {

    private final TimeWindowReservoirBuilder reservoirBuilder;

    public TimeWindowMetricBuilderFactory() {
        this(15, TimeUnit.SECONDS);
    }

    public TimeWindowMetricBuilderFactory(long window, TimeUnit windowUnit) {
        this(1, TimeUnit.SECONDS, window, windowUnit);
    }

    public TimeWindowMetricBuilderFactory(long flushPeriod, TimeUnit flushUnit, long window, TimeUnit windowUnit) {
        this(SlidingExponentialDecayingReservoir.builder()
                .flushEvery(flushPeriod, flushUnit)
                .window(window, windowUnit));
    }

    public TimeWindowMetricBuilderFactory(TimeWindowReservoirBuilder reservoirBuilder) {
        this.reservoirBuilder = reservoirBuilder;
    }

    private final MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
        @Override
        public Histogram newMetric() {
            return new Histogram(reservoirBuilder.build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Histogram.class.isInstance(metric);
        }
    };


    private final MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
        @Override
        public Timer newMetric() {
            return new Timer(reservoirBuilder.build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Timer.class.isInstance(metric);
        }
    };

    private final MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
        @Override
        public Counter newMetric() {
            return new Counter();
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Counter.class.isInstance(metric);
        }
    };

    private final MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
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