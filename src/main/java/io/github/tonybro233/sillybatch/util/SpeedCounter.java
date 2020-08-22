package io.github.tonybro233.sillybatch.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counter with speed sampling
 */
public class SpeedCounter {

    private final LongAdder counter;

    private final long sampleInterval;

    private volatile double speed;

    private long lastCount;

    private final AtomicLong lastTime;

    public SpeedCounter(long sampleInterval) {
        this.sampleInterval = sampleInterval;
        this.counter = new LongAdder();
        this.speed = 0;
        this.lastCount = 0;
        this.lastTime = new AtomicLong();
    }

    public void increment() {
        add(1);
    }

    public void add(long val) {
        speedSampling();
        counter.add(val);
    }

    public long count() {
        speedSampling();
        return this.counter.sum();
    }

    public double currentRawSpeed() {
        speedSampling();
        return speed;
    }

    public String currentSpeed() {
        return currentSpeed(TimeUnit.SECONDS, "record");
    }

    public String currentSpeed(TimeUnit timeUnit, String baseUnit) {
        speedSampling();
        switch (timeUnit) {
            case NANOSECONDS:
                return String.format("%f %s/%s", speed / 1000_000, baseUnit, "ns");
            case MICROSECONDS:
                return String.format("%f %s/%s", speed / 1000, baseUnit, "μs");
            case MILLISECONDS:
                return String.format("%f %s/%s", speed, baseUnit, "ms");
            case SECONDS:
                return String.format("%.2f %s/%s", speed * 1000, baseUnit, "s");
            case MINUTES:
                return String.format("%.2f %s/%s", speed * 60_000, baseUnit, "m");
            case HOURS:
                return String.format("%.2f %s/%s", speed * 3600_000, baseUnit, "h");
            case DAYS:
                return String.format("%.2f %s/%s", speed * 24 * 3600_000, baseUnit, "d");
            default:
                throw new IllegalArgumentException("Unknown time unit");
        }
    }

    private void speedSampling() {
        final long lastSampleTime = lastTime.get();
        final long currentTime = System.currentTimeMillis();
        if (currentTime - sampleInterval >= lastSampleTime &&
                lastTime.compareAndSet(lastSampleTime, currentTime)) {
            if (lastSampleTime != 0) {
                final long countSnapshot = counter.sum();
                speed = ((double) (countSnapshot - lastCount)) / (currentTime - lastSampleTime);
                lastCount = countSnapshot;
            }
        }
    }

}
