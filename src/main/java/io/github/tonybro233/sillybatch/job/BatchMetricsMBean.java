package io.github.tonybro233.sillybatch.job;

import java.time.LocalDateTime;

public interface BatchMetricsMBean {

    long getReadCount();

    String getReadSpeed();

    long getProcessCount();

    String getProcessSpeed();

    long getWriteCount();

    String getWriteSpeed();

    long getFilterCount();

    long getErrorCount();

    LocalDateTime getStartTime();

    Long getTotal();

}
