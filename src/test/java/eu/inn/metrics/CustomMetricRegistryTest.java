package eu.inn.metrics;

import org.testng.annotations.Test;

public class CustomMetricRegistryTest {

    @Test
    public void defaultBuilderCouldCreateAllMetrics() {
        CustomMetricRegistry registry = CustomMetricRegistry.builder().build();
        registry.timer("timer");
        registry.histogram("histogram");
        registry.meter("meter");
        registry.counter("counter");
    }
}
