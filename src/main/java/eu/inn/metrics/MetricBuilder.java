package eu.inn.metrics;

import com.codahale.metrics.Metric;


public interface MetricBuilder<T extends Metric> {

    public T newMetric();

    public boolean isInstance(Metric metric);
}