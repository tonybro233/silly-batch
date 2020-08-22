package io.github.tonybro233.sillybatch.listener;

import io.github.tonybro233.sillybatch.writer.RecordWriter;

public interface RecordWriteListener<T> {

    /**
     * Called before {@link RecordWriter#write(T)}
     */
    void beforeWrite(T record);

    /**
     * Called after {@link RecordWriter#write(T)}
     */
    void afterWrite(T record);

    /**
     * Called if an exception was thrown from {@link RecordWriter#write(T)}.
     */
    void onWriteError(Exception exception, T record);

}
