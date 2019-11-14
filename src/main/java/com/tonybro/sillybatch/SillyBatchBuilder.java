package com.tonybro.sillybatch;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class SillyBatchBuilder<I, O> {

    private String batchName;

    private CompositeRecordReader<I> reader;

    private CompositeRecordWriter<O> writer;

    private CompositeRecordProcessor<I, O> processor;

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
        this.writer = new CompositeRecordWriter<>();
        this.processor = new CompositeRecordProcessor<>();
    }

    private <K> SillyBatchBuilder(SillyBatchBuilder<I, K> builder, RecordProcessor<K, O> processor) {
        this.batchName = builder.batchName;
        this.reader = builder.reader;
        this.processor = builder.processor.addProcessor(processor);
        this.writer = new CompositeRecordWriter<>();
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

    public static <I> SillyBatchBuilder<I, I> newBuilder() {
        return new SillyBatchBuilder<>(null);
    }

    public static <I> SillyBatchBuilder<I, I> newBuilder(Class<I> inputClazz) {
        return new SillyBatchBuilder<>(null);
    }

    public static <I> SillyBatchBuilder<I, I> newBuilder(String batchName) {
        return new SillyBatchBuilder<>(batchName);
    }

    public SillyBatchBuilder<I, O> name(String name) {
        this.batchName = name;
        return this;
    }

    public SillyBatchBuilder<I, O> addReader(RecordReader<I> reader) {
        this.reader.addReader(reader);
        return this;
    }

    public SillyBatchBuilder<I, O> addWriter(RecordWriter<O> writer) {
        this.writer.addWriter(writer);
        return this;
    }

    public <K> SillyBatchBuilder<I, K> addProcessor(RecordProcessor<O, K> processor) {
        if (this.writer.size() > 0) {
            throw new IllegalStateException("Cannot add processor as writer has been set!");
        }
        return new SillyBatchBuilder<>(this, processor);
    }

    public SillyBatchBuilder<I, O> parallelRead(Boolean parallelRead) {
        this.parallelRead = parallelRead;
        return this;
    }

    public SillyBatchBuilder<I, O> parallelRead(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.readExecutor = executor;
        this.parallelRead = true;
        return this;
    }

    public SillyBatchBuilder<I, O> parallelProcess(boolean parallelProcess) {
        this.parallelProcess = parallelProcess;
        return this;
    }

    public SillyBatchBuilder<I, O> parallelProcess(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.processExecutor = executor;
        this.parallelProcess = true;
        return this;
    }

    public SillyBatchBuilder<I, O> parallelWrite(boolean parallelWrite) {
        this.parallelWrite = parallelWrite;
        return this;
    }

    public SillyBatchBuilder<I, O> parallelWrite(ExecutorService executor) {
        if (null == executor) {
            throw new NullPointerException();
        }
        this.writeExecutor = executor;
        this.parallelWrite = true;
        return this;
    }

    public SillyBatchBuilder<I, O> forceOrder(boolean forceOrder) {
        this.forceOrder = forceOrder;
        return this;
    }

    public SillyBatchBuilder<I, O> chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public SillyBatchBuilder<I, O> failover(long failover) {
        this.failover = failover;
        return this;
    }

    public SillyBatchBuilder<I, O> poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public SillyBatchBuilder<I, O> report(boolean report) {
        this.report = report;
        return this;
    }

    public SillyBatchBuilder<I, O> reportInterval(long interval) {
        this.reportInterval = interval;
        return this;
    }

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
