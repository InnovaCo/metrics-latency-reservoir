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
        assertEmpty(reservoir);
    }

    @Test
    public void nonEmptyReservoir_CollectAllMetricsFromTheFirstWindow() throws InterruptedException {
        LatencyReservoir reservoir = createReservoir(100);

        final int metricsCount = 100;

        track(reservoir, metricsCount);

        Thread.sleep(110);

        assertNonEmpty(reservoir, metricsCount);
    }

    @Test
    public void nonEmptyReservoir_BecomesEmptyAfterOldWindowsSlided() throws InterruptedException {
        LatencyReservoir reservoir = createReservoir(10);
        final int metricsCount = 100;

        track(reservoir, metricsCount);

        Thread.sleep(150);

        assertEmpty(reservoir);
    }

    @Test
    public void nonEmptyReservoir_BecomesOperationalAfterSliding() throws InterruptedException {
        LatencyReservoir reservoir = createReservoir(10);
        track(reservoir, 1000);
        Thread.sleep(150);
        assertEmpty(reservoir);

        track(reservoir, 10);
        Thread.sleep(20);
        assertNonEmpty(reservoir, 10);
    }

    private LatencyReservoir createReservoir(long flushInMillis) {
        LatencyStats stats = LatencyStats.Builder.create()
                .lowestTrackableLatency(1)
                .highestTrackableLatency(Long.MAX_VALUE)
                .build();

        return LatencyReservoir.builder().stats(stats).flush(10, TimeUnit.MILLISECONDS).build();
    }

    private void track(LatencyReservoir reservoir, int metricsCount) {
        for (long i = 1; i <= metricsCount; i++) {
            reservoir.update(i);
        }
    }

    private void assertEmpty(LatencyReservoir reservoir) {
        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Empty reservoir size should be 0", 0, reservoir.size());
        assertEquals("Empty snapshot size should be 0", 0, snapshot.size());
        assertEquals("Empty snapshot min should be 0", 0, snapshot.getMin());
        assertEquals("Empty snapshot max should be 0", 0, snapshot.getMax());
    }

    private void assertNonEmpty(LatencyReservoir reservoir, int max) {
        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Unexpected reservoir size", max, reservoir.size());
        assertEquals("Unexpected snapshot size", max, snapshot.size());
        assertEquals("Unexpected snapshot min", 1, snapshot.getMin());
        assertEquals("Unexpected snapshot max", max, snapshot.getMax());
    }
}
