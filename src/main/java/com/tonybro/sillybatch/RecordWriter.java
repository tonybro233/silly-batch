package com.tonybro.sillybatch;

import java.util.Collection;

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
     * Write a record
     *
     * @param record record to write.
     * @throws Exception if an error occurs during record writing
     */
    void write(T record) throws Exception;

    /**
     * Write a chunk of records.
     * The default implementation is calling {@link RecordWriter#write(T record)} by loop.
     *
     * @param records records to write
     * @throws Exception Exception if an error occurs during writing
     */
    default void write(Collection<T> records) throws Exception {
        for (T record : records) {
            write(record);
        }
    }

}
