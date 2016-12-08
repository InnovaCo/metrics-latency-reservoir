package eu.inn.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import eu.inn.metrics.hdr.HdrLatencyReservoir;
import eu.inn.metrics.sed.SlidingExponentialDecayingReservoir;
import org.LatencyUtils.LatencyStats;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

public class LatencyReservoirTest {

    @Test
    public void emptyReservoir() {
        HdrLatencyReservoir reservoir = HdrLatencyReservoir.builder().build();
        assertEmpty(reservoir);
    }

    @Test(dataProvider = "longReservoirs")
    public void nonEmptyReservoir_CollectAllMetricsFromTheFirstWindow(Reservoir reservoir) throws InterruptedException {
        final int metricsCount = 100;

        track(reservoir, metricsCount);

        Thread.sleep(110);

        assertNonEmpty(reservoir, metricsCount);
    }

    @Test(dataProvider = "shortReservoirs")
    public void nonEmptyReservoir_BecomesEmptyAfterOldWindowsSlided(Reservoir reservoir) throws InterruptedException {
        final int metricsCount = 100;

        track(reservoir, metricsCount);

        Thread.sleep(150);

        assertEmpty(reservoir);
    }

    @Test(dataProvider = "shortReservoirs")
    public void nonEmptyReservoir_BecomesOperationalAfterSliding(Reservoir reservoir) throws InterruptedException {
        track(reservoir, 1000);
        Thread.sleep(150);
        assertEmpty(reservoir);

        track(reservoir, 10);
        Thread.sleep(20);
        assertNonEmpty(reservoir, 10);
    }

    @DataProvider(name = "longReservoirs")
    public static Object[][] longReservoirs() {
        return reservoirs(100);
    }

    @DataProvider(name = "shortReservoirs")
    public static Object[][] shortReservoirs() {
        return reservoirs(10);
    }

    public static Object[][] reservoirs(long flushInMillis) {
        return new Object[][] {
                {createHdrReservoir(flushInMillis)},
                {createTimeSlidingReservoir(flushInMillis)}
        };
    }

    private static Reservoir createHdrReservoir(long flushInMillis) {
        LatencyStats stats = LatencyStats.Builder.create()
                .lowestTrackableLatency(1)
                .highestTrackableLatency(Long.MAX_VALUE)
                .build();

        return HdrLatencyReservoir.builder()
                .stats(stats)
                .flushEvery(flushInMillis, TimeUnit.MILLISECONDS)
                .window(120, TimeUnit.MILLISECONDS)
               .build();
    }

    private static Reservoir createTimeSlidingReservoir(long flushInMillis) {
        return SlidingExponentialDecayingReservoir.builder()
                .flushEvery(flushInMillis, TimeUnit.MILLISECONDS)
                .window(120, TimeUnit.MILLISECONDS)
                .build();
    }

    private void track(Reservoir reservoir, int metricsCount) {
        for (long i = 1; i <= metricsCount; i++) {
            reservoir.update(i);
        }
    }

    private void assertEmpty(Reservoir reservoir) {
        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Empty reservoir size should be 0", 0, reservoir.size());
        assertEquals("Empty snapshot size should be 0", 0, snapshot.size());
        assertEquals("Empty snapshot min should be 0", 0, snapshot.getMin());
        assertEquals("Empty snapshot max should be 0", 0, snapshot.getMax());
    }

    private void assertNonEmpty(Reservoir reservoir, int max) {
        Snapshot snapshot = reservoir.getSnapshot();

        assertEquals("Unexpected reservoir size", max, reservoir.size());
        assertEquals("Unexpected snapshot size", max, snapshot.size());
        assertEquals("Unexpected snapshot min", 1, snapshot.getMin());
        assertEquals("Unexpected snapshot max", max, snapshot.getMax());
    }
}
