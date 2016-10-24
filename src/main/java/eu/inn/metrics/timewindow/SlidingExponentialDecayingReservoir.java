package eu.inn.metrics.timewindow;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.WeightedSnapshot;
import eu.inn.metrics.staff.NamedThreadFactory;
import eu.inn.metrics.staff.Sink;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SlidingExponentialDecayingReservoir implements Reservoir {

    private final Sink<Collection<WeightedSnapshot.WeightedSample>> sink;

    private volatile ExponentiallyDecayingReservoir currentReservoir = new ExponentiallyDecayingReservoir();

    private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory());

    private final long flushPeriod;

    private final TimeUnit flushUnit;

    private static Field valuesAccessor;

    static {
        try {
            valuesAccessor = ExponentiallyDecayingReservoir.class.getDeclaredField("values");
            valuesAccessor.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public SlidingExponentialDecayingReservoir(long flushPeriod, TimeUnit flushUnit, int sinkSize) {
        this.flushPeriod = flushPeriod;
        this.flushUnit = flushUnit;
        this.sink = new Sink<>(sinkSize);
        scheduleHistogramFlush();
    }

    @Override
    public int size() {
        int size = 0;
        for (Collection<WeightedSnapshot.WeightedSample> s : sink.getAll()) {
            size += s.size();
        }
        System.out.println("SIZE " + size);
        return size;
    }

    @Override
    public void update(long value) {
        currentReservoir.update(value);
    }

    @Override
    public Snapshot getSnapshot() {
        ArrayList<WeightedSnapshot.WeightedSample> weightedSamples = new ArrayList<>(size());
        for (Collection<WeightedSnapshot.WeightedSample> s : sink.getAll()) {
            weightedSamples.addAll(s);
        }
        return new WeightedSnapshot(weightedSamples);
    }

    private void scheduleHistogramFlush() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Collection<WeightedSnapshot.WeightedSample> valuesSnapshot;
                if (currentReservoir.size() > 0) {
                    try {
                        ExponentiallyDecayingReservoir reservoirSnapshot = currentReservoir;
                        currentReservoir = new ExponentiallyDecayingReservoir();
                        valuesSnapshot = ((Map<Double, WeightedSnapshot.WeightedSample>) valuesAccessor.get(reservoirSnapshot)).values();
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    valuesSnapshot = null;
                }
                sink.add(valuesSnapshot);
            }
        }, flushPeriod, flushPeriod, flushUnit);
    }

    public static class Builder {

        private long flushPeriod = 1;
        private TimeUnit flushUnit = TimeUnit.SECONDS;

        private int sinkSize;
        private long window;
        private TimeUnit windowUnit;

        private final int DEFAULT_SINK_SIZE = 15;

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

        public SlidingExponentialDecayingReservoir build() {
            if (windowUnit != null && sinkSize != 0) {
                throw new IllegalArgumentException("Either window parameters or sinkSize should be set");
            }
            if (windowUnit != null) {
                sinkSize = (int) Math.ceil((double) windowUnit.toNanos(window) / flushUnit.toNanos(flushPeriod));
            }

            if (sinkSize == 0) {
                sinkSize = DEFAULT_SINK_SIZE;
            }
            return new SlidingExponentialDecayingReservoir(flushPeriod, flushUnit, sinkSize);
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
