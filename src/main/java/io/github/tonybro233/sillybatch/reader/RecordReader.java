package io.github.tonybro233.sillybatch.reader;

import java.util.List;

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
     * Whether reader support reading chunk of records
     *
     * @return true if support reading chunk of records
     */
    default boolean supportReadChunk() {
        return false;
    }

    /**
     * Read next chunk of records from the data source.
     * Don't forget to override {@link RecordReader#supportReadChunk()} <br>
     * Note that if this method throws an exception, the error counter of
     * batch will only increase by one.
     *
     * @return the next chunk of record from the data source or null if the end of the data source is reached
     * @throws Exception if an error occurs during reading
     */
    default List<T> readChunk() throws Exception {
        throw new UnsupportedOperationException();
    }


}
