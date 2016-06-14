package eu.inn.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.LatencyUtils.LatencyStats;

public class LatencyReservoir implements Reservoir {

    private final LatencyStats stats;

    public LatencyReservoir() {
        this(new LatencyStats());
    }

    public LatencyReservoir(LatencyStats stats) {
        this.stats = stats;
    }

    @Override
    public int size() {
        return getSnapshot().size();
    }

    @Override
    public void update(long value) {
        stats.recordLatency(value);
    }

    @Override
    public Snapshot getSnapshot() {
        return new HistogramSnapshot(stats.getIntervalHistogram());
    }
}