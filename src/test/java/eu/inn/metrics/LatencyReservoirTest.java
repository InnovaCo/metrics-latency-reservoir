package eu.inn.metrics;

import com.codahale.metrics.Snapshot;
import org.LatencyUtils.LatencyStats;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

public class LatencyReservoirTest {

    @Test
    public void emptyReservoir() {
        LatencyReservoir reservoir = LatencyReservoir.builder().build();
        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Empty reservoir size should be 0", 0, reservoir.size());
        assertEquals("Empty snapshot size should be 0", 0, snapshot.size());
        assertEquals("Empty snapshot min should be 0", 0, snapshot.getMin());
        assertEquals("Empty snapshot max should be 0", 0, snapshot.getMax());
    }

    @Test
    public void nonEmptyReservoir_CollectAllMetricsFromTheFirstWindow() throws InterruptedException {
        LatencyStats stats = LatencyStats.Builder.create()
                .lowestTrackableLatency(1)
                .highestTrackableLatency(Long.MAX_VALUE)
            .build();

        LatencyReservoir reservoir = LatencyReservoir.builder().stats(stats).flush(100, TimeUnit.MILLISECONDS).build();
        final int metricsCount = 100;

        for (long i = 1; i <= metricsCount; i++) {
            reservoir.update(i);
        }

        Thread.sleep(110);

        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Unexpected reservoir size", metricsCount, reservoir.size());
        assertEquals("Unexpected snapshot size", metricsCount, snapshot.size());
        assertEquals("Unexpected snapshot min", 1, snapshot.getMin());
        assertEquals("Unexpected snapshot max", metricsCount, snapshot.getMax());
    }

    @Test
    public void nonEmptyReservoir_BecomesEmptyAfterOldWindowsSlided() throws InterruptedException {
        LatencyStats stats = LatencyStats.Builder.create()
                .lowestTrackableLatency(1)
                .highestTrackableLatency(Long.MAX_VALUE)
                .build();

        LatencyReservoir reservoir = LatencyReservoir.builder().stats(stats).flush(10, TimeUnit.MILLISECONDS).build();
        final int metricsCount = 100;

        for (long i = 1; i <= metricsCount; i++) {
            reservoir.update(i);
        }

        Thread.sleep(150);

        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Empty reservoir size should be 0", 0, reservoir.size());
        assertEquals("Empty snapshot size should be 0", 0, snapshot.size());
        assertEquals("Empty snapshot min should be 0", 0, snapshot.getMin());
        assertEquals("Empty snapshot max should be 0", 0, snapshot.getMax());
    }
}
