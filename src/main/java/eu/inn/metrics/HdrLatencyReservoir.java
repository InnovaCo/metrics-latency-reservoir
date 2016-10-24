package eu.inn.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import eu.inn.metrics.staff.NamedThreadFactory;
import eu.inn.metrics.staff.Sink;
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
    public static class Builder {
        private LatencyStats stats = new LatencyStats();

        private long flushPeriod = 5;
        private TimeUnit flushUnit = TimeUnit.SECONDS;

        private int sinkSize;
        private long window;
        private TimeUnit windowUnit;

        private final int DEFAULT_SINK_SIZE = 12;

        public Builder stats(LatencyStats stats) {
            this.stats = stats;
            return this;
        }

        public Builder flush(long flushPeriod, TimeUnit flushUnit) {
            validatePeriods("flushPeriod", flushPeriod, flushUnit);
            this.flushPeriod = flushPeriod;
            this.flushUnit = flushUnit;
            return this;
        }

        public Builder sinkSize(int sinkSize) {
            if (sinkSize < 1) {
                throw new IllegalArgumentException("sinkSize should be positive integer");
            }
            this.sinkSize = sinkSize;
            return this;
        }

        public Builder window(long window, TimeUnit windowUnit) {
            validatePeriods("window", window, windowUnit);
            this.window = window;
            this.windowUnit = windowUnit;
            return this;
        }

        public HdrLatencyReservoir build() {
            if (windowUnit != null && sinkSize != 0) {
                throw new IllegalArgumentException("Either window parameters or sinkSize should be set");
            }
            if (windowUnit != null) {
                sinkSize = (int) Math.ceil((double) windowUnit.toNanos(window) / flushUnit.toNanos(flushPeriod));
            }

            if (sinkSize == 0) {
                sinkSize = DEFAULT_SINK_SIZE;
            }
            return new HdrLatencyReservoir(stats, flushPeriod, flushUnit, sinkSize);
        }

        private static void validatePeriods(String name, long period, TimeUnit unit) {
            if (period <= 0) {
                throw new IllegalArgumentException(name + " duration should be positive integer");
            }
            if (unit == null) {
                throw new IllegalArgumentException(name + " unit should be non-null");
            }
        }
    }
}

