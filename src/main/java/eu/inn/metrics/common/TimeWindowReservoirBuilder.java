package eu.inn.metrics.common;

import com.codahale.metrics.Reservoir;

import java.util.concurrent.TimeUnit;

public abstract class TimeWindowReservoirBuilder<T extends Reservoir> {

    protected long flushPeriod = 1;
    protected TimeUnit flushUnit = TimeUnit.SECONDS;

    protected long window = 15;
    protected TimeUnit windowUnit = TimeUnit.SECONDS;

    public TimeWindowReservoirBuilder<T> flushEvery(long flushPeriod, TimeUnit flushUnit) {
        validatePeriods("flushPeriod", flushPeriod, flushUnit);
        this.flushPeriod = flushPeriod;
        this.flushUnit = flushUnit;
        return this;
    }

    public TimeWindowReservoirBuilder<T> window(long window, TimeUnit windowUnit) {
        validatePeriods("window", window, windowUnit);
        this.window = window;
        this.windowUnit = windowUnit;
        return this;
    }

    abstract public T build();

    private static void validatePeriods(String name, long period, TimeUnit unit) {
        if (period <= 0) {
            throw new IllegalArgumentException(name + " duration should be positive integer");
        }
        if (unit == null) {
            throw new IllegalArgumentException(name + " unit should be non-null");
        }
    }
}