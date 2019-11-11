package com.tonybro.sillybatch;

import java.util.Collection;

public interface RecordReader<T> {

    /**
     * Open the reader.
     *
     * @throws Exception if an error occurs during reader opening
     */
    default void open() throws Exception { }

    /**
     * Close the reader.
     *
     * @throws Exception if an error occurs during reader closing
     */
    default void close() throws Exception { }

    /**
     * Read next record from the data source.
     *
     * @return the next record from the data source or null if the end of the data source is reached
     * @throws Exception if an error occurs during reading next record
     */
    T read() throws Exception;

    /**
     * Get total records if possible. If reader can't get the number, just return null.
     *
     * @return the count of records to be read or null if not possible.
     * @throws Exception if an error occurs during calculate
     */
    default Long getTotal() throws Exception {
        return null;
    }

    /**
     * whether reader support reading chunk of records
     */
    default boolean supportReadChunk() {
        return false;
    }

    /**
     * read next chunk of records from the data source
     *
     * @param size chunk size
     * @return the next chunk of record from the data source or null if the end of the data source is reached
     * @throws Exception if an error occurs during reading
     */
    default Collection<T> readChunk(int size) throws Exception {
        throw new UnsupportedOperationException();
    }


}
