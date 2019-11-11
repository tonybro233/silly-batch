package com.tonybro.sillybatch;

public interface RecordProcessor<I, O> {

    /**
     * Open the processor.
     *
     * @throws Exception if an error occurs during opening the writer
     */
    default void open() throws Exception { }

    /**
     * Close the processor
     *
     * @throws Exception if an error occurs during closing the writer
     */
    default void close() throws Exception { }

    /**
     * Process a record.
     *
     * @param record to process.
     * @return the processed record, may be of another type of the input record, or null to skip
     * @throws Exception if an error occurs during record processing
     */
    O processRecord(I record) throws Exception;
}
