package com.tonybro.sillybatch.listener;

import com.tonybro.sillybatch.writer.RecordWriter;

import java.util.List;

public interface RecordWriteListener<T> {

    /**
     * Called before {@link RecordWriter#write(List)}
     */
    void beforeWrite(List<? extends T> records);

    /**
     * Called after {@link RecordWriter#write(List)}
     */
    void afterWrite(List<? extends T> records);

    /**
     * Called if an exception was thrown from {@link RecordWriter#write(List)}.
     */
    void onWriteError(Exception exception, List<? extends T> records);

}
