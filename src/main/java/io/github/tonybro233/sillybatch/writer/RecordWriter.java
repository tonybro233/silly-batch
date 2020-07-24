package io.github.tonybro233.sillybatch.writer;

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
     * Write a record.
     *
     * @param record record to write
     * @throws Exception Exception if an error occurs during writing
     */
    void write(T record) throws Exception;

}
