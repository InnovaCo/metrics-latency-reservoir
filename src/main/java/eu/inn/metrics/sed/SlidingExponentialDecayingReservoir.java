package eu.inn.metrics.sed;

import com.codahale.metrics.*;
import eu.inn.metrics.common.NamedThreadFactory;
import eu.inn.metrics.common.Sink;
import eu.inn.metrics.common.TimeWindowReservoirBuilder;

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

    public static SlidingExponentialDecayingReservoir.Builder builder() {
        return new Builder();
    }

    public static class Builder extends TimeWindowReservoirBuilder<SlidingExponentialDecayingReservoir> {

        public SlidingExponentialDecayingReservoir build() {
            int sinkSize = (int) Math.ceil((double) windowUnit.toNanos(window) / flushUnit.toNanos(flushPeriod));
            return new SlidingExponentialDecayingReservoir(flushPeriod, flushUnit, sinkSize);
        }
    }
}
