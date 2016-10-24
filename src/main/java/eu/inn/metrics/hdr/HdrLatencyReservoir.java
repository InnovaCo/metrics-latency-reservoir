package eu.inn.metrics.hdr;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import eu.inn.metrics.common.NamedThreadFactory;
import eu.inn.metrics.common.Sink;
import eu.inn.metrics.common.TimeWindowReservoirBuilder;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;

import java.util.List;
import java.util.concurrent.*;

/**
 * @todo
 *  1. provide a method for returning only new histograms for a concrete reporter
 *  2. make an immutable histogram class and use its empty instance instead of the domestic option
 */
public class HdrLatencyReservoir implements Reservoir {

    private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory());

    private final static HistogramSnapshot emptyHistogramSnapshot = new HistogramSnapshot(new Histogram(0));

    private final LatencyStats stats;

    private final long flushPeriod;

    private final TimeUnit flushUnit;

    private final Sink<Histogram> sink;

    private volatile boolean valueAddedSinceSnapshotTaken = false;

    public HdrLatencyReservoir(LatencyStats stats, long flushPeriod, TimeUnit flushUnit, int sinkSize) {
        this.stats = stats;
        this.flushPeriod = flushPeriod;
        this.flushUnit = flushUnit;
        this.sink = new Sink<>(sinkSize);
        scheduleHistogramFlush();
    }

    @Override
    public int size() {
        int size = 0;
        for (Histogram h: sink.getAll()) {
            size += h.getTotalCount();
        }
        return size;
    }

    @Override
    public void update(long value) {
        valueAddedSinceSnapshotTaken = true;
        stats.recordLatency(value);
    }

    @Override
    public Snapshot getSnapshot() {
        long highestTrackableValue = 0;
        long lowestDiscernibleValue = Long.MAX_VALUE;
        int numberOfSignificantValueDigits = 0;

        List<Histogram> histogramList = sink.getAll();

        for (Histogram h: histogramList) {
            if (h.getHighestTrackableValue() > highestTrackableValue) {
                highestTrackableValue = h.getHighestTrackableValue();
            }
            if (h.getLowestDiscernibleValue() < lowestDiscernibleValue) {
                lowestDiscernibleValue = h.getLowestDiscernibleValue();
            }
            if (h.getNumberOfSignificantValueDigits() > numberOfSignificantValueDigits) {
                numberOfSignificantValueDigits = h.getNumberOfSignificantValueDigits();
            }
        }
        if (highestTrackableValue == 0) {
            return emptyHistogramSnapshot;
        } else {
            Histogram mergedHistogram = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
            for (Histogram h: histogramList) {
                mergedHistogram.add(h);
            }
            return new HistogramSnapshot(mergedHistogram);
        }
    }

    private void scheduleHistogramFlush() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Histogram histogram = null;
                if (valueAddedSinceSnapshotTaken) {
                    valueAddedSinceSnapshotTaken = false; // a possible race condition here
                    histogram = stats.getIntervalHistogram();
                    if (histogram.getTotalCount() == 0) {
                        histogram = null;
                    }
                }
                sink.add(histogram);
            }
        }, flushPeriod, flushPeriod, flushUnit);
    }

    public static HdrLatencyReservoir.Builder builder() {
        return new Builder();
    }

    /**
     * @todo synchronize LatencyStats settings with a window and a flush settings
     */
    public static class Builder extends TimeWindowReservoirBuilder<HdrLatencyReservoir> {

        private LatencyStats stats = new LatencyStats();

        public Builder stats(LatencyStats stats) {
            this.stats = stats;
            return this;
        }

        public HdrLatencyReservoir build() {
            int sinkSize = (int) Math.ceil((double) windowUnit.toNanos(window) / flushUnit.toNanos(flushPeriod));
            return new HdrLatencyReservoir(stats, flushPeriod, flushUnit, sinkSize);
        }
    }
}

