package eu.inn.metrics;

import com.codahale.metrics.*;

public interface MetricBuilderFactory {

    public MetricBuilder<Histogram> histogramsBuilder();

    public MetricBuilder<Timer> timersBuilder();

    public MetricBuilder<Counter> countersBuilder();

    public MetricBuilder<Meter> metersBuilder();
}
