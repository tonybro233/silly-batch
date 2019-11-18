package io.github.tonybro233.sillybatch.job;

import io.github.tonybro233.sillybatch.listener.*;
import io.github.tonybro233.sillybatch.processor.CompositeRecordProcessor;
import io.github.tonybro233.sillybatch.processor.RecordProcessor;
import io.github.tonybro233.sillybatch.reader.CompositeRecordReader;
import io.github.tonybro233.sillybatch.reader.RecordReader;
import io.github.tonybro233.sillybatch.writer.CompositeRecordWriter;
import io.github.tonybro233.sillybatch.writer.RecordWriter;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Builder of silly batch, enable configuring and creating batch
 * by fluent api.
 *
 * <p>This Builder provided composite implementations of core
 * interfaces and listener interfaces, you can add same kind of
 * component more than once.
 *
 * @author tony
 */
public final class SillyBatchBuilder<I, O> {

    private String batchName;

    private CompositeRecordReader<I> reader;

    private CompositeRecordProcessor<I, O> processor;

    private CompositeRecordWriter<O> writer;

    private CompositeRecordReadListener<I> readListener;

    private CompositeRecordProcessListener<I, O> processListener;

    private CompositeRecordWriteListener<O> writeListener;

    private Boolean parallelRead;

    private Boolean parallelProcess;

    private Boolean parallelWrite;

    private Boolean forceOrder;

    private Integer chunkSize;

    private Long failover;

    private Integer poolSize;

    private ExecutorService readExecutor;

    private ExecutorService processExecutor;

    private ExecutorService writeExecutor;

    private Boolean report;

    private Long reportInterval;


    private SillyBatchBuilder(String batchName) {
        this.batchName = batchName;
        this.reader = new CompositeRecordReader<>();
        this.processor = new CompositeRecordProcessor<>();
        this.writer = new CompositeRecordWriter<>();
        this.readListener = new CompositeRecordReadListener<>();
        this.processListener = new CompositeRecordProcessListener<>();
        this.writeListener = new CompositeRecordWriteListener<>();
    }

    private <K> SillyBatchBuilder(SillyBatchBuilder<I, K> builder, RecordProcessor<? super K, O> processor) {
        this.batchName = builder.batchName;
        this.reader = builder.reader;
        this.processor = builder.processor.addProcessor(processor);
        this.writer = new CompositeRecordWriter<>();
        this.readListener = builder.readListener;
        this.processListener = new CompositeRecordProcessListener<>();
        this.writeListener = new CompositeRecordWriteListener<>();
        this.parallelRead = builder.parallelRead;
        this.parallelProcess = builder.parallelProcess;
        this.parallelWrite = builder.parallelWrite;
        this.forceOrder = builder.forceOrder;
        this.chunkSize = builder.chunkSize;
        this.failover = builder.failover;
        this.poolSize = builder.poolSize;
        this.readExecutor = builder.readExecutor;
        this.processExecutor = builder.processExecutor;
        this.writeExecutor = builder.writeExecutor;
        this.report = builder.report;
        this.reportInterval = builder.reportInterval;
    }

    /**
     * Create a new builder. Example:<br/>
     * {@code SillyBatchBuilder.<Integer>newBuilder() }
     *
     * @param <I> the type of record read by reader
     */
    public static <I> SillyBatchBuilder<I, I> newBuilder() {
        return new SillyBatchBuilder<>(null);
    }

    /**
     * Create a new builder. Example:<br/>
     * {@code SillyBatchBuilder.newBuilder(Integer.class) }
     *
     * @param inputClazz the type of record read by reader
     * @param <I> the type of record read by reader
     */
    public static <I> SillyBatchBuilder<I, I> newBuilder(Class<I> inputClazz) {
        return new SillyBatchBuilder<>(null);
    }

    /**
     * Create a new builder with specified name. Example: <br/>
     * {@code SillyBatchBuilder.<Integer>newBuilder("name") }
     *
     * @param batchName the name of batch
     * @param <I> the type of record read by reader
     */
    public static <I> SillyBatchBuilder<I, I> newBuilder(String batchName) {
        return new SillyBatchBuilder<>(batchName);
    }

    /**
     * Set the name of batch
     */
    public SillyBatchBuilder<I, O> name(String name) {
        this.batchName = name;
        return this;
    }

    /**
     * Add a record reader. You can add more than one reader.
     *
     * @see CompositeRecordReader
     */
    public SillyBatchBuilder<I, O> addReader(RecordReader<? extends I> reader) {
        this.reader.addReader(reader);
        return this;
    }

    /**
     * Add a record processor. You can add more than one processor.
     *
     * @see CompositeRecordProcessor
     */
    public <K> SillyBatchBuilder<I, K> addProcessor(RecordProcessor<? super O, K> processor) {
        if (this.writer.size() > 0) {
            throw new IllegalStateException("Cannot add processor after writer has been set!");
        }
        if (this.processListener.size() > 0) {
            throw new IllegalStateException("Cannot add processor after process listener has been set!");
        }
        if (this.writeListener.size() > 0) {
            throw new IllegalStateException("Cannot add processor after writer listener has been set!");
        }
        return new SillyBatchBuilder<>(this, processor);
    }

    /**
     * Add a record writer. You can add more than one writer.
     *
     * @see CompositeRecordWriter
     */
    public SillyBatchBuilder<I, O> addWriter(RecordWriter<? super O> writer) {
        this.writer.addWriter(writer);
        return this;
    }

    /**
     * Add a record read listener. You can add more than one listener.
     *
     * @see CompositeRecordReadListener
     */
    public SillyBatchBuilder<I, O> addListener(RecordReadListener<? super I> listener) {
        this.readListener.addListener(listener);
        return this;
    }

    /**
     * Add a record process listener. You can add more than one listener.
     *
     * @see CompositeRecordProcessListener
     */
    public SillyBatchBuilder<I, O> addListener(RecordProcessListener<? super I, ? super O> listener) {
        this.processListener.addListener(listener);
        return this;
    }

    /**
     * Add a record write listener. You can add more than one listener.
     *
     * @see CompositeRecordWriteListener
     */
    public SillyBatchBuilder<I, O> addListener(RecordWriteListener<? super O> listener) {
        this.writeListener.addListener(listener);
        return this;
    }

    /**
     * Read record in parallel mode (or not). Be sure the data source
     * support parallel read and added readers, read listeners are thread safe.
     * The default value is false.
     */
    public SillyBatchBuilder<I, O> parallelRead(boolean parallelRead) {
        this.parallelRead = parallelRead;
        return this;
    }

    /**
     * Read record in parallel mode and specify the executor. Be sure
     * the data source support parallel read and added readers, read
     * listeners are thread safe.
     */
    public SillyBatchBuilder<I, O> parallelRead(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.readExecutor = executor;
        this.parallelRead = true;
        return this;
    }

    /**
     * Process record in parallel mode (or not). Be sure that the added
     * processors, process listeners are thread safe.
     * The default value is false.
     */
    public SillyBatchBuilder<I, O> parallelProcess(boolean parallelProcess) {
        this.parallelProcess = parallelProcess;
        return this;
    }

    /**
     * Process record in parallel mode and specify the executor. Be sure
     * that the added processors, process listeners are thread safe.
     */
    public SillyBatchBuilder<I, O> parallelProcess(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.processExecutor = executor;
        this.parallelProcess = true;
        return this;
    }

    /**
     * Write record in parallel mode (or not). Be sure that the added
     * writers, write listeners are thread safe.
     * The default value is false.
     */
    public SillyBatchBuilder<I, O> parallelWrite(boolean parallelWrite) {
        this.parallelWrite = parallelWrite;
        return this;
    }

    /**
     * Write record in parallel mode and specify the executor. Be sure
     * that the added writers, write listeners are thread safe.
     */
    public SillyBatchBuilder<I, O> parallelWrite(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.writeExecutor = executor;
        this.parallelWrite = true;
        return this;
    }

    /**
     * Process records after all records been read and write records after
     * all records been processed.
     * The default value is false.
     */
    public SillyBatchBuilder<I, O> forceOrder(boolean forceOrder) {
        this.forceOrder = forceOrder;
        return this;
    }

    /**
     * Make reader and writer handle records in chunks (if supported).
     * The default value is 1.
     *
     * @param chunkSize size of chunk
     * @return
     */
    public SillyBatchBuilder<I, O> chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    /**
     * Set the error threshold of the batch. Batch will be aborted if the
     * error count larger than the failover.
     * The default value is 0. (Means abort batch as soon as error occurred)
     */
    public SillyBatchBuilder<I, O> failover(long failover) {
        this.failover = failover;
        return this;
    }

    /**
     * Set the pool size of executor created by silly batch. (When you choose
     * parallel mode and not provide executor)
     * The default value is {@code Runtime.getRuntime().availableProcessors() * 2 }
     */
    public SillyBatchBuilder<I, O> poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Whether report metrics continuously by logging.
     * The default value is true.
     */
    public SillyBatchBuilder<I, O> report(boolean report) {
        this.report = report;
        return this;
    }

    /**
     * Set the interval of logging report (milli second).
     * The default value is 2000.
     */
    public SillyBatchBuilder<I, O> reportInterval(long interval) {
        this.reportInterval = interval;
        return this;
    }

    /**
     * Build a silly batch instance.
     */
    public SillyBatch<I, O> build() {
        if (reader.size() == 0) {
            throw new IllegalStateException("You must assign a reader");
        }
        // allow no writer
        // if (writer.size() == 0) {
        //     throw new IllegalStateException("You must assign a writer");
        // }
        SillyBatch<I, O> batch = new SillyBatch<>();
        if (reader.size() == 1) {
            batch.setReader(reader.getReader(0));
        } else {
            batch.setReader(reader);
        }
        batch.setProcessor(processor);
        if (writer.size() == 1) {
            batch.setWriter(writer.getWriter(0));
        } else {
            batch.setWriter(writer);
        }
        if (readListener.size() > 0) {
            batch.setListener(readListener);
        }
        if (processListener.size() > 0) {
            batch.setListener(processListener);
        }
        if (writeListener.size() > 0) {
            batch.setListener(writeListener);
        }
        Optional.ofNullable(batchName).ifPresent(batch::setName);
        Optional.ofNullable(parallelRead).ifPresent(batch::setParallelRead);
        Optional.ofNullable(parallelProcess).ifPresent(batch::setParallelProcess);
        Optional.ofNullable(parallelWrite).ifPresent(batch::setParallelWrite);
        Optional.ofNullable(forceOrder).ifPresent(batch::setForceOrder);
        Optional.ofNullable(chunkSize).ifPresent(batch::setChunkSize);
        Optional.ofNullable(failover).ifPresent(batch::setFailover);
        Optional.ofNullable(poolSize).ifPresent(batch::setPoolSize);
        Optional.ofNullable(readExecutor).ifPresent(batch::setParallelRead);
        Optional.ofNullable(processExecutor).ifPresent(batch::setParallelProcess);
        Optional.ofNullable(writeExecutor).ifPresent(batch::setParallelWrite);
        Optional.ofNullable(report).ifPresent(batch::setReport);
        Optional.ofNullable(reportInterval).ifPresent(batch::setReportInterval);

        return batch;
    }

}
