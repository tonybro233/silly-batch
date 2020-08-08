package io.github.tonybro233.sillybatch.writer;

import java.util.ArrayList;
import java.util.List;

/**
 * Base record writer with buffer. If it is more efficient to
 * write chunk of records at a time for you, then you can
 * implement your writer by inheriting this class for convenience.
 *
 * <p><b>NOTICE</b>: Writing is delayed by buffer flush, this
 * may affect the fault tolerance of the batch, and last records
 * may not written until writer is closed.
 *
 * @param <T> generic type
 */
public abstract class BufferedRecordWriter<T> implements RecordWriter<T> {

    private static final int DEFAULT_SIZE = 128;

    private int size;

    private List<T> buffer;

    public BufferedRecordWriter() {
        this(DEFAULT_SIZE);
    }

    public BufferedRecordWriter(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.size = size;
    }

    @Override
    public final void open() throws Exception {
        buffer = new ArrayList<>(size);
        doOpen();
    }

    /**
     * See {@link RecordWriter#open()}
     */
    public abstract void doOpen() throws Exception;

    @Override
    public void close() throws Exception {
        if (null != buffer && !buffer.isEmpty()) {
            try {
                flushBuffer();
            } finally {
                buffer = null;
                doClose();
            }
        } else {
            doClose();
        }
    }

    /**
     * See {@link RecordWriter#close()}
     */
    public abstract void doClose() throws Exception;

    @Override
    public final void write(T record) throws Exception {
        if (buffer.size() == size) {
            flushBuffer();
        }
        buffer.add(record);
    }

    private synchronized void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        flush(buffer);
        buffer.clear();
    }

    /**
     * Flushes the output buffer
     *
     * @param records buffer to write
     */
    protected abstract void flush(List<T> records);

}
