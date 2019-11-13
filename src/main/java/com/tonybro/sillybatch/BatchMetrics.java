package com.tonybro.sillybatch;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class BatchMetrics implements Serializable {

    private volatile LocalDateTime startTime;

    private volatile LocalDateTime endTime;

    private volatile Long total;

    private AtomicLong readCount = new AtomicLong();

    private AtomicLong processCount = new AtomicLong();

    private AtomicLong filterCount = new AtomicLong();

    private AtomicLong writeCount = new AtomicLong();

    private AtomicLong errorCount = new AtomicLong();

    public String report() {
        String str = "{Read: " + readCount.get()
                + ", Processed: " + processCount.get()
                + ", Filtered: " + filterCount.get()
                + ", Written: " + writeCount.get()
                + ", Failed: " + errorCount.get();
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
        str += ", readCount=" + readCount.get()
                + ", processCount=" + processCount.get()
                + ", filterCount=" + filterCount.get()
                + ", writeCount=" + writeCount.get()
                + ", errorCount=" + errorCount.get()
                + '}';
        return str;
    }

    public long getReadCount() {
        return readCount.get();
    }

    public long getProcessCount() {
        return processCount.get();
    }

    public long getFilterCount() {
        return filterCount.get();
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    public void incrementReadCount() {
        readCount.incrementAndGet();
    }

    public void incrementProcessCount() {
        processCount.incrementAndGet();
    }

    public void incrementFilterCount() {
        filterCount.incrementAndGet();
    }

    public void incrementWriteCount() {
        writeCount.incrementAndGet();
    }

    public void incrementErrorCount() {
        errorCount.incrementAndGet();
    }

    public void addReadCount(long delta) {
        readCount.addAndGet(delta);
    }

    public void addProcessCount(long delta) {
        writeCount.addAndGet(delta);
    }

    public void addFilterCount(long delta) {
        filterCount.addAndGet(delta);
    }

    public void addWriteCount(long delta) {
        writeCount.addAndGet(delta);
    }

    public void addErrorCount(long delta) {
        errorCount.addAndGet(delta);
    }

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
