package com.tonybro.sillybatch.writer;

import java.util.List;

public interface RecordWriter<T> {

    /**
     * Open the writer.
     *
     * @throws Exception if an error occurs during opening the writer
     */
    default void open() throws Exception { }

    /**
     * Close the writer
     *
     * @throws Exception if an error occurs during closing the writer
     */
    default void close() throws Exception { }

    /**
     * Write a chunk of records.
     *
     * @param records records to write
     * @throws Exception Exception if an error occurs during writing
     */
    void write(List<? extends T> records) throws Exception;

}
