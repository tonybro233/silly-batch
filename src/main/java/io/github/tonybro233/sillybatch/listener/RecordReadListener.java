package io.github.tonybro233.sillybatch.listener;

import io.github.tonybro233.sillybatch.reader.RecordReader;

import java.util.List;

public interface RecordReadListener<T> {

    /**
     * Called before {@link RecordReader#read()} and {@link RecordReader#readChunk()}
     */
    void beforeRead();

    /**
     * Called after {@link RecordReader#read()}.
     * Not called when the reader returns null.
     */
    void afterRead(T record);

    /**
     * Called after {@link RecordReader#readChunk()}.
     * Not called when the reader returns null.
     * The default implementation is calling {@link RecordReadListener#afterRead(T record)} by loop.
     */
    default void afterRead(List<? extends T> records) {
        for (T record : records) {
            afterRead(record);
        }
    }

    /**
     * Called if an error occurs while trying to read.
     */
    void onReadError(Exception ex);

}
