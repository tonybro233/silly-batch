package com.tonybro.sillybatch;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SillyBatch<I, O> {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SillyBatch.class);


    // 傻批
    private String name = "SillyBatch";

    /* ------------------------- core -------------------------- */

    private RecordReader<I> reader;

    private RecordWriter<O> writer;

    private RecordProcessor<I, O> processor;

    /* ------------------------- param -------------------------- */

    private boolean parallelRead = false;

    private boolean parallelProcess = false;

    private boolean parallelWrite = false;

    private boolean forceOrder = false;

    private int chunkSize = 1;

    private long failover = 0;

    // report metrics continuously by logging
    private boolean report = true;

    // report interval
    private long reportInterval = 2000;

    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;

    /* --------------------- multi thread ---------------------- */

    private ExecutorService executor;

    private Queue<Future<?>> readJobQueue;

    private Queue<Future<?>> processJobQueue;

    private Queue<Future<?>> writeJobQueue;

    private LinkedBlockingQueue<I> readQueue;

    private LinkedBlockingDeque<O> writeQueue;

    private CountDownLatch readOverLatch;

    /* ------------------------- mark -------------------------- */

    private boolean externalExecutor = false;

    private volatile boolean readOver;

    private volatile boolean readFinished;

    private volatile boolean processFinished;

    private volatile boolean forceClean;

    private volatile boolean started = false;

    private volatile boolean readChunk = false;

    private int bufferSize;

    private AtomicBoolean aborted;

    private AtomicLong jobSeq;

    /* ----------------------- assistant -------------------------- */

    private BatchMetrics metrics;

    private BatchReporter reporter;

    private ReadManager readManager;

    private ProcessManager processManager;

    private WriteManager writeManager;

    private Thread mainThread;

    /* ------------------------- const -------------------------- */

    private static final long QUEUE_WAIT = 500L;

    private static final long SHUTDOWN_WAIT = 10000L;

    private static final double THRESHOLD = 1.1D;

    /* ------------------------- main -------------------------- */

    public final int execute() {
        try {
            prepare();

            if (forceOrder) {
                readManager.start();
                readManager.join();

                processManager.start();
                processManager.join();

                writeManager.start();
                writeManager.join();
            } else {
                readManager.start();
                processManager.start();
                writeManager.start();

                readManager.join();
                processManager.join();
                writeManager.join();
            }
        } catch (InterruptedException ignore) {
            // interrupter will LOGGER the exception
            aborted.set(true);
            return 1;
        } catch (Exception e) {
            aborted.set(true);
            LOGGER.error("Error occurred.", e);
            return 1;
        } finally {
            // clean context
            teardown();
        }

        return 0;
    }

    private void prepare() throws Exception {
        if (started) {
            throw new IllegalStateException("This batch instance has already started!");
        }
        LOGGER.info("Prepare executing {} !\nparallelRead={}, parallelProcess={}, parallelWrite={}, \n"
                        + "forceOrder={}, chunk={}, failover={}, poolSize={}",
                name, parallelRead, parallelProcess, parallelWrite,
                forceOrder, chunkSize, failover, executor == null ? poolSize : "External");

        mainThread = Thread.currentThread();
        metrics = new BatchMetrics();
        reporter = new BatchReporter();
        started = true;
        forceClean = false;
        readOver = false;
        readOverLatch = new CountDownLatch(1);
        readFinished = false;
        processFinished = false;
        aborted = new AtomicBoolean(false);
        jobSeq = new AtomicLong();
        readChunk = chunkSize > 1 && reader.supportReadChunk();
        bufferSize = Math.max(10, chunkSize);

        readQueue = new LinkedBlockingQueue<>();
        writeQueue = new LinkedBlockingDeque<>();

        if (parallelRead) {
            readJobQueue = new LinkedList<>();
        }
        if (parallelProcess) {
            processJobQueue = new LinkedList<>();
        }
        if (parallelWrite) {
            writeJobQueue = new LinkedList<>();
        }

        if (parallelRead || parallelProcess || parallelWrite) {
            if (null == executor) {
                if (forceOrder) {
                    executor = new ThreadPoolExecutor(
                            poolSize, poolSize,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            new BasicThreadFactory.Builder().namingPattern("sb-pool-%d").build());
                } else {
                    /*
                      The reason to use PriorityThreadPoolExecutor and PriorityBlockingQueue
                      is while using unbound LinkedBlockingQueue, if reading record is much
                      faster than processing and writing (which is common), processing jobs
                      will all be submitted soon, and it cause writing jobs wait for all
                      processing job done.
                    */
                    executor = new PriorityThreadPoolExecutor(
                            poolSize, poolSize,
                            0L, TimeUnit.MILLISECONDS,
                            new PriorityBlockingQueue<>(),
                            new BasicThreadFactory.Builder().namingPattern("sb-pool-%d").build());
                }
            } else {
                externalExecutor = true;
            }
        }

        // create manager
        readManager = new ReadManager();
        processManager = new ProcessManager();
        writeManager = new WriteManager();

        // open reader、writer、processor
        openReader();
        openWriter();
        openProcessor();

        metrics.setTotal(reader.getTotal());
        metrics.setStartTime(LocalDateTime.now());

        LOGGER.info("Execution started ...");
        reporter.start();
    }

    private void teardown() {
        readOver = true;
        metrics.setEndTime(LocalDateTime.now());
        if (null != reporter && reporter.isAlive()) {
            reporter.interrupt();
        }

        if (readManager.isAlive()) {
            readManager.interrupt();
        }
        if (processManager.isAlive()) {
            processManager.interrupt();
        }
        if (writeManager.isAlive()) {
            writeManager.interrupt();
        }

        if (null != executor) {
            if (externalExecutor) {
                interruptJobs(readJobQueue);
                interruptJobs(processJobQueue);
                interruptJobs(writeJobQueue);
            } else {
                if (!executor.isShutdown()) {
                    LOGGER.info("Terminating executor ...");
                    executor.shutdown();
                }
                try {
                    executor.awaitTermination(SHUTDOWN_WAIT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.error("Waiting termination interrupted.");
                }
                if (!executor.isTerminated()) {
                    LOGGER.info("Waiting termination timeout, into force clean mode ...");
                    forceClean = true;
                    executor.shutdownNow();
                }
            }
        }

        // clear queue
        Optional.ofNullable(readQueue).ifPresent(Queue::clear);
        Optional.ofNullable(writeQueue).ifPresent(Queue::clear);
        Optional.ofNullable(readJobQueue).ifPresent(Queue::clear);
        Optional.ofNullable(processJobQueue).ifPresent(Queue::clear);
        Optional.ofNullable(writeJobQueue).ifPresent(Queue::clear);

        // close reader、writer、processor
        closeReader();
        closeWriter();
        closeProcessor();

        started = false;
        LOGGER.info("Execution {}!\n{}", aborted.get() ? "failed" : "succeeded", metrics.toString());
    }

    private Collection<I> doRead() {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }
        try {
            Collection<I> records = null;
            if (readChunk) {
                records = reader.readChunk(chunkSize);
                if (null != records) {
                    metrics.addReadCount(records.size());
                }
            } else {
                I record = reader.read();
                if (null != record) {
                    metrics.incrementReadCount();
                    records = Collections.singletonList(record);
                }
            }
            return records;
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("Error reading records", e);
                metrics.addErrorCount(readChunk ? chunkSize : 1);
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("Error occurred while stop reading forcibly.", e);
            }
            return Collections.emptyList();
        }
    }

    private void doProcess(I record) {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        try {
            O out = processor.processRecord(record);
            if (null == out) {
                metrics.incrementFilterCount();
            } else {
                metrics.incrementProcessCount();
                writeQueue.offer(out);
            }
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("Exception in processing record", e);
                metrics.incrementErrorCount();
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("Error occurred while stop processing forcibly.", e);
            }
        }
    }

    private void doWrite(List<O> records) {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        try {
            writer.write(records);
            metrics.addWriteCount(records.size());
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("Exception in writing records", e);
                metrics.addErrorCount(records.size());
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("Error occurred while stop writing forcibly.", e);
            }
        }
    }

    private void processRecord(I record) {
        if (parallelProcess) {
            processJobQueue.offer(executor.submit(new RecordProcessJob(record)));
        } else {
            doProcess(record);
        }
    }

    private void writeRecords(List<O> records) {
        if (parallelWrite) {
            writeJobQueue.offer(executor.submit(new RecordWriteJob(records)));
        } else {
            doWrite(records);
        }
    }

    private void waitForJobs(Queue<Future<?>> queue) throws InterruptedException {
        if (null != queue) {
            Future<?> future;
            while ((future = queue.poll()) != null) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    LOGGER.error("Unexpected error while waiting job complete.", e);
                }
            }
        }
    }

    private void interruptJobs(Queue<Future<?>> queue) {
        if (null != queue) {
            Future<?> future;
            while ((future = queue.poll()) != null) {
                future.cancel(true);
            }
        }
    }

    private static class FailOverExceededException extends RuntimeException {
        public FailOverExceededException() {
            super("Exceed failover, abort execution.");
        }
    }

    private static class BatchAbortedException extends RuntimeException {
        public BatchAbortedException() {
            super("Batch has been aborted.");
        }
    }

    /* ------------------------- thread & callable ------------------------- */

    private final class BatchReporter extends Thread {

        private BatchMetrics last;

        BatchReporter() {
            super("watcher");
        }

        @Override
        public void run() {
            if (!report) {
                return;
            }
            while (true) {
                try {
                    sleep(reportInterval);
                    if (!metrics.equals(last)) {
                        last = metrics.copy();
                        LOGGER.info(metrics.report());
                    }
                } catch (InterruptedException ignore) {
                    return;
                }
            }
        }
    }

    private final class ReadManager extends Thread {

        ReadManager() {
            super("read-manager");
        }

        @Override
        public void run() {
            LOGGER.info("Read manager started ...");
            try {
                if (parallelRead) {
                    for (int i = 0; i <= poolSize; i++) {
                        readJobQueue.offer(executor.submit(new RecordReadJob()));
                    }
                    readOverLatch.await();
                    waitForJobs(readJobQueue);
                } else {
                    while (true) {
                        if (aborted.get()) {
                            return;
                        }
                        Collection<I> records = doRead();
                        if (null != records) {
                            for (I record : records) {
                                readQueue.offer(record);
                            }
                        } else {
                            break;
                        }
                    }
                }
            } catch (InterruptedException | BatchAbortedException ignore) {
                // InterruptedException: only main thread will interrupt manager
                // BatchAbortedException: stopped by processor or writer, just return
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while reading records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("Read manager stopped.");
                readFinished = true;
            }
        }

    }

    private final class ProcessManager extends Thread {

        ProcessManager() {
            super("process-manager");
        }

        @Override
        public void run() {
            LOGGER.info("Process manager started ...");
            try {
                I record;
                while (!readFinished) {
                    record = readQueue.poll(QUEUE_WAIT, TimeUnit.MILLISECONDS);
                    if (null != record) {
                        processRecord(record);
                    }
                    if (aborted.get()) {
                        return;
                    }
                }

                // flush
                while ((record = readQueue.poll()) != null) {
                    processRecord(record);
                    if (aborted.get()) {
                        return;
                    }
                }

                waitForJobs(processJobQueue);
            } catch (InterruptedException | BatchAbortedException ignore) {
                // InterruptedException: only main thread will interrupt manager
                // BatchAbortedException: stopped by reader or writer, just return
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while processing records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("Process manager stopped.");
                processFinished = true;
            }
        }
    }

    private final class WriteManager extends Thread {

        WriteManager() {
            super("write-manager");
        }

        @Override
        public void run() {
            LOGGER.info("Write manager started ...");
            try {
                O record;
                List<O> buffer = new ArrayList<>(bufferSize);
                while (!processFinished) {
                    record = writeQueue.poll(QUEUE_WAIT, TimeUnit.MILLISECONDS);
                    if (null != record) {
                        buffer.add(record);
                        if (buffer.size() == chunkSize) {
                            writeRecords(buffer);
                            buffer = new ArrayList<>(bufferSize);
                        }
                    }

                    if (aborted.get()) {
                        return;
                    }
                }

                // flush
                while ((record = writeQueue.poll()) != null) {
                    buffer.add(record);
                    if (buffer.size() == chunkSize) {
                        writeRecords(buffer);
                        buffer = new ArrayList<>(bufferSize);
                    }

                    if (aborted.get()) {
                        return;
                    }
                }

                if (!buffer.isEmpty()) {
                    writeRecords(buffer);
                }

                waitForJobs(writeJobQueue);
            } catch (InterruptedException | BatchAbortedException ignore) {
                // InterruptedException: only main thread will interrupt manager
                // BatchAbortedException: stopped by reader or processor, just return
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while writing records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("Write manager stopped.");
            }
        }
    }

    private abstract class BatchJob implements Runnable, Comparable<BatchJob> {

        private final int priority;

        private final long seq;

        public BatchJob(int priority) {
            this.priority = priority;
            this.seq = jobSeq.getAndIncrement();
        }

        @Override
        public int compareTo(BatchJob other) {
            int res = this.priority - other.priority;
            if (res == 0) {
                res = (seq < other.seq ? -1 : 1);
            }
            return res;
        }
    }

    private class RecordReadJob extends BatchJob {

        static final int PRIORITY = 2;

        public RecordReadJob() {
            super(PRIORITY);
        }

        @Override
        public void run() {
            if (readOver || aborted.get()) {
                return;
            }
            try {
                Collection<I> records = doRead();
                if (null != records) {
                    for (I record : records) {
                        readQueue.offer(record);
                    }
                } else {
                    readOver = true;
                    readOverLatch.countDown();
                }
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while reading records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                if (!readOver && !aborted.get()) {
                    readJobQueue.offer(executor.submit(new RecordReadJob()));
                }
            }
        }
    }

    private class RecordProcessJob extends BatchJob {

        static final int PRIORITY = 1;

        I record;

        public RecordProcessJob(I record) {
            super(PRIORITY);
            this.record = record;
        }

        @Override
        public void run() {
            if (aborted.get()) {
                return;
            }
            try {
                doProcess(record);
            } catch (FailOverExceededException e) {
                // prevent repeat log
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while processing record, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            }
        }
    }

    private class RecordWriteJob extends BatchJob {

        static final int PRIORITY = 0;

        List<O> records;

        public RecordWriteJob(List<O> records) {
            super(PRIORITY);
            this.records = records;
        }

        @Override
        public void run() {
            if (aborted.get()) {
                return;
            }
            try {
                doWrite(records);
            } catch (FailOverExceededException e) {
                // prevent repeat log
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while writing record, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            }
        }
    }

    /* --------------------- open & close ---------------------- */

    private void openReader() throws Exception {
        LOGGER.info("Opening record reader ...");
        try {
            reader.open();
        } catch (Exception e) {
            LOGGER.error("Unable to open record reader", e);
            throw new Exception("Shutdown");
        }
    }

    private void openWriter() throws Exception {
        LOGGER.info("Opening record writer ...");
        try {
            writer.open();
        } catch (Exception e) {
            LOGGER.error("Unable to open record writer", e);
            throw new Exception("Shutdown");
        }
    }

    private void openProcessor() throws Exception {
        if (null != processor) {
            LOGGER.info("Opening record processor ...");
            try {
                processor.open();
            } catch (Exception e) {
                LOGGER.error("Unable to open record processor", e);
                throw new Exception("Shutdown");
            }
        }
    }

    private void closeReader() {
        try {
            LOGGER.info("Closing record reader ...");
            reader.close();
        } catch (Exception e) {
            LOGGER.error("Unable to close record reader", e);
        }
    }

    private void closeWriter() {
        try {
            LOGGER.info("Closing record writer ...");
            writer.close();
        } catch (Exception e) {
            LOGGER.error("Unable to close record writer", e);
        }
    }

    private void closeProcessor() {
        if (null != processor) {
            try {
                LOGGER.info("Closing record processor ...");
                processor.close();
            } catch (Exception e) {
                LOGGER.error("Unable to close record processor", e);
            }
        }
    }

    /* --------------------- setter ---------------------- */

    public void setName(String name) {
        this.name = name;
    }

    public void setReader(RecordReader<I> reader) {
        this.reader = reader;
    }

    public void setWriter(RecordWriter<O> writer) {
        this.writer = writer;
    }

    public void setProcessor(RecordProcessor<I, O> processor) {
        this.processor = processor;
    }

    public void setParallelRead(boolean parallelRead) {
        this.parallelRead = parallelRead;
    }

    public void setParallelProcess(boolean parallelProcess) {
        this.parallelProcess = parallelProcess;
    }

    public void setParallelWrite(boolean parallelWrite) {
        this.parallelWrite = parallelWrite;
    }

    public void setForceOrder(boolean ordered) {
        this.forceOrder = ordered;
    }

    public void setChunkSize(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("ChunkSize must be positive!");
        }
        this.chunkSize = chunkSize;
    }

    public void setFailover(long failover) {
        if (failover < 0) {
            throw new IllegalArgumentException("Failover must not be negative!");
        }
        this.failover = failover;
    }

    public void setPoolSize(int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("PoolSize must be positive!");
        }
        this.poolSize = poolSize;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setReport(boolean report) {
        this.report = report;
    }

    public void setReportInterval(long reportInterval) {
        if (reportInterval <= 0) {
            throw new IllegalArgumentException("Interval must be positive");
        }
        this.reportInterval = reportInterval;
    }
}