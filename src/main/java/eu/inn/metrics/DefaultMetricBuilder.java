package eu.inn.metrics;

import com.codahale.metrics.*;

public class DefaultMetricBuilder {

    public static final MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
        @Override
        public Histogram newMetric() {
            return new Histogram(LatencyReservoir.builder().build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Histogram.class.isInstance(metric);
        }
    };


    public static final MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
        @Override
        public Timer newMetric() {
            return new Timer(LatencyReservoir.builder().build());
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Timer.class.isInstance(metric);
        }
    };

    public static final MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
        @Override
        public Counter newMetric() {
            return new Counter();
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Counter.class.isInstance(metric);
        }
    };

    public static final MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
        @Override
        public Meter newMetric() {
            return new Meter();
        }

        @Override
        public boolean isInstance(Metric metric) {
            return Meter.class.isInstance(metric);
        }
    };
}
