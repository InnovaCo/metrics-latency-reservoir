package eu.inn.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @todo provide a method for returning only new histograms for a concrete reporter
 */
public class LatencyReservoir implements Reservoir {

    private final LatencyStats stats;

    private final long flushPeriod;

    private final TimeUnit flushUnit;

    private final int sinkSize;

    private final LinkedBlockingQueue<Histogram> sink;

    private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory());

    public LatencyReservoir(LatencyStats stats, long flushPeriod, TimeUnit flushUnit, int sinkSize) {
        this.stats = stats;
        this.flushPeriod = flushPeriod;
        this.flushUnit = flushUnit;
        this.sinkSize = sinkSize;
        this.sink = new LinkedBlockingQueue<>(sinkSize);
        scheduleHistogramFlush();
    }

    @Override
    public int size() {
        Histogram[] histograms = sink.toArray(new Histogram[0]);
        int size = 0;
        for (Histogram h: histograms) {
            size += h.getTotalCount();
        }
        return size;
    }

    @Override
    public void update(long value) {
        stats.recordLatency(value);
    }

    @Override
    public Snapshot getSnapshot() {
        Histogram mergedHistogram = mergeHistogram();
        return new HistogramSnapshot(mergedHistogram);
    }

    private Histogram mergeHistogram() {
        Histogram[] histograms = sink.toArray(new Histogram[0]);

        /**
         * lowest possible values taken directly from
         * the org.HdrHistogram.AbstractHistogram constructor
         */
        long highestTrackableValue = 2;
        long lowestDiscernibleValue = 1;
        int numberOfSignificantValueDigits = 0;

        for (Histogram h: histograms) {
            if (h.getHighestTrackableValue() > highestTrackableValue) {
                highestTrackableValue = h.getHighestTrackableValue();
            }
            if (h.getLowestDiscernibleValue() > lowestDiscernibleValue) {
                lowestDiscernibleValue = h.getLowestDiscernibleValue();
            }
            if (h.getNumberOfSignificantValueDigits() > numberOfSignificantValueDigits) {
                numberOfSignificantValueDigits = h.getNumberOfSignificantValueDigits();
            }
        }
        Histogram mergedHistogram = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        for (Histogram h: histograms) {
            mergedHistogram.add(h);
        }
        return mergedHistogram;
    }

    private void scheduleHistogramFlush() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Histogram histogram = stats.getIntervalHistogram();
                if (sink.size() == sinkSize) {
                    sink.poll();
                }
                sink.add(histogram);
            }
        }, flushPeriod, flushPeriod, flushUnit);
    }

    public static LatencyReservoir.Builder builder() {
        return new Builder();
    }

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

        public LatencyReservoir build() {
            if (windowUnit != null && sinkSize != 0) {
                throw new IllegalArgumentException("Either window parameters or sinkSize should be set");
            }
            if (windowUnit != null) {
                sinkSize = (int) Math.ceil((double) windowUnit.toNanos(window) / flushUnit.toNanos(flushPeriod));
            }

            if (sinkSize == 0) {
                sinkSize = DEFAULT_SINK_SIZE;
            }
            return new LatencyReservoir(stats, flushPeriod, flushUnit, sinkSize);
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

class NamedThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    NamedThreadFactory() {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = "latency-reservoir-dump-pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        t.setDaemon(true);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}