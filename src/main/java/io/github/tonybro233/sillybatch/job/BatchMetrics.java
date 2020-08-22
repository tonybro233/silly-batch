package io.github.tonybro233.sillybatch.job;

import io.github.tonybro233.sillybatch.util.SpeedCounter;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics of batch job.
 */
public class BatchMetrics implements BatchMetricsMBean, Serializable {

    private volatile LocalDateTime startTime;

    private volatile LocalDateTime endTime;

    private volatile Long total;

    private SpeedCounter readCount = new SpeedCounter(1500);

    private SpeedCounter processCount = new SpeedCounter(1500);

    private SpeedCounter writeCount = new SpeedCounter(1500);

    private LongAdder filterCount = new LongAdder();

    private LongAdder errorCount = new LongAdder();

    public String report() {
        String str = "{Read: " + readCount.count()
                + ", Processed: " + processCount.count()
                + ", Written: " + writeCount.count()
                + ", Filtered: " + filterCount.sum()
                + ", Failed: " + errorCount.sum();
        if (null != total) {
            str += ", total: " + total;
        }
        return str + "}";
    }

    @Override
    public String toString() {
        String str = "SBMetrics{"
                + "startTime=" + startTime
                + ", endTime=" + endTime;
        if (null != startTime && null != endTime) {
            str += ", duration=" + Duration.between(startTime, endTime);
        }
        str += ", readCount=" + readCount.count()
                + ", processCount=" + processCount.count()
                + ", writeCount=" + writeCount.count()
                + ", filterCount=" + filterCount.sum()
                + ", errorCount=" + errorCount.sum()
                + '}';
        return str;
    }

    @Override
    public long getReadCount() {
        return readCount.count();
    }

    @Override
    public String getReadSpeed() {
        return readCount.currentSpeed();
    }

    @Override
    public long getProcessCount() {
        return processCount.count();
    }

    @Override
    public String getProcessSpeed() {
        return processCount.currentSpeed();
    }

    @Override
    public long getWriteCount() {
        return writeCount.count();
    }

    @Override
    public String getWriteSpeed() {
        return writeCount.currentSpeed();
    }

    @Override
    public long getFilterCount() {
        return filterCount.sum();
    }

    @Override
    public long getErrorCount() {
        return errorCount.sum();
    }

    public void incrementReadCount() {
        readCount.increment();
    }

    public void incrementProcessCount() {
        processCount.increment();
    }

    public void incrementFilterCount() {
        filterCount.increment();
    }

    public void incrementWriteCount() {
        writeCount.increment();
    }

    public void incrementErrorCount() {
        errorCount.increment();
    }

    public void addReadCount(long delta) {
        readCount.add(delta);
    }

    public void addProcessCount(long delta) {
        processCount.add(delta);
    }

    public void addFilterCount(long delta) {
        filterCount.add(delta);
    }

    public void addWriteCount(long delta) {
        writeCount.add(delta);
    }

    public void addErrorCount(long delta) {
        errorCount.add(delta);
    }

    @Override
    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    @Override
    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BatchMetrics that = (BatchMetrics) o;
        return getReadCount() == that.getReadCount()
                && getProcessCount() == that.getProcessCount()
                && getFilterCount() == that.getFilterCount()
                && getWriteCount() == that.getWriteCount()
                && getErrorCount() == that.getErrorCount()
                && Objects.equals(getTotal(), that.getTotal())
                && Objects.equals(getStartTime(), that.getStartTime())
                && Objects.equals(getEndTime(), that.getEndTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStartTime(), getEndTime(), getTotal(),
                getReadCount(), getProcessCount(), getFilterCount(), getWriteCount(), getErrorCount());
    }

    public BatchMetrics copy() {
        BatchMetrics metrics = new BatchMetrics();
        metrics.setStartTime(this.getStartTime());
        metrics.setEndTime(this.getEndTime());
        metrics.addReadCount(this.getReadCount());
        metrics.addProcessCount(this.getProcessCount());
        metrics.addFilterCount(this.getFilterCount());
        metrics.addWriteCount(this.getWriteCount());
        metrics.addErrorCount(this.getErrorCount());
        metrics.setTotal(this.getTotal());
        return metrics;
    }

}
