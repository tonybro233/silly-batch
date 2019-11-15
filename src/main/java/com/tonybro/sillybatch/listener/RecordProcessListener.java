package com.tonybro.sillybatch.listener;

import com.tonybro.sillybatch.processor.RecordProcessor;

public interface RecordProcessListener<I, O> {

    /**
     * Called before {@link RecordProcessor#process(Object)}.
     */
    void beforeProcess(I record);

    /**
     * Called after {@link RecordProcessor#process(Object)} returns.
     * This method will still be called even if the processor returns {@code null}.
     */
    void afterProcess(I record, O result);

    /**
     * Called if an exception was thrown from {@link RecordProcessor#process(Object)}.
     */
    void onProcessError(Exception e, I record);

}
