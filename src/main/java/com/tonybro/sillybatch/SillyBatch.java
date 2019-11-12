package com.tonybro.sillybatch;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private int chunkSize = 1;

    private long failover = 0;

    // report metrics continuously by logging
    private boolean report = true;

    // report interval
    private long reportInterval = 2000;

    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;

    /* --------------------- multi thread ---------------------- */

    private ExecutorService executor;

    private Queue<Future<Boolean>> readJobQueue;

    private Queue<Future<Boolean>> processJobQueue;

    private Queue<Future<Boolean>> writeJobQueue;

    private LinkedBlockingQueue<I> readQueue;

    private LinkedBlockingDeque<O> writeQueue;

    private CountDownLatch readOverLatch;

    /* ------------------------- mark -------------------------- */

    private volatile boolean readOver;

    private volatile boolean readFinished;

    private volatile boolean processFinished;

    private volatile boolean forceClean;

    private volatile boolean started = false;

    private volatile boolean readChunk = false;

    private int bufferSize;

    private AtomicBoolean aborted;

    /* ----------------------- assistant -------------------------- */

    private BatchMetrics metrics;

    private BatchReporter reporter;

    private HandleManager handleManager;

    private Thread mainThread;

    /* ------------------------- const -------------------------- */

    private static final long QUEUE_WAIT = 500L;

    private static final long SHUTDOWN_WAIT = 10000L;

    private static final double THRESHOLD = 1.1D;

    /* ------------------------- main -------------------------- */

    public final int execute() {
        try {
            prepare();

            if (parallelRead) {
                // start reading
                for (int i = 0; i <= poolSize; i++) {
                    readJobQueue.offer(executor.submit(new RecordReadJob()));
                }

                // start writing
                handleManager.start();

                // wait finish submit reading job
                readOverLatch.await();

                // wait reading complete
                Future<Boolean> future;
                while (true) {
                    future = readJobQueue.poll();
                    if (null != future) {
                        future.get();
                    } else {
                        break;
                    }
                }
                readFinished = true;

                // wait finish submit writing job (parallel write) or writing complete
                handleManager.join();
            } else {
                List<I> buffer = new ArrayList<>(bufferSize);
                while (true) {
                    // read
                    Collection<I> records = doRead();
                    if (null != records) {
                        buffer.addAll(records);
                    } else {
                        break;
                    }

                    if (aborted.get()) {
                        return 1;
                    }

                    // write
                    if (buffer.size() > chunkSize * THRESHOLD) {
                        processAndWriteRecords(new ArrayList<>(buffer.subList(0, chunkSize)));
                        buffer = new ArrayList<>(buffer.subList(chunkSize, buffer.size()));
                    } else if (buffer.size() >= chunkSize) {
                        processAndWriteRecords(buffer);
                        buffer = new ArrayList<>(bufferSize);
                    }
                }

                if (aborted.get()) {
                    return 1;
                }

                // flush
                while (buffer.size() > chunkSize * THRESHOLD) {
                    processAndWriteRecords(new ArrayList<>(buffer.subList(0, chunkSize)));
                    buffer = new ArrayList<>(buffer.subList(chunkSize, buffer.size()));
                }
                if (!buffer.isEmpty()) {
                    processAndWriteRecords(buffer);
                }
            }

            if (aborted.get()) {
                return 1;
            }

            if (parallelWrite) {
                LOGGER.info("Writing job all submitted, shutdown executor and waiting complete ...");
                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
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
        LOGGER.info("Prepare executing {} ! parallelRead={}, parallelWrite={}, poolSize={}, chunk={}, failover={}",
                name, parallelRead, parallelWrite, poolSize, chunkSize, failover);

        mainThread = Thread.currentThread();
        metrics = new BatchMetrics();
        reporter = new BatchReporter();
        started = true;
        forceClean = false;
        readOver = false;
        readOverLatch = new CountDownLatch(1);
        readFinished = false;
        aborted = new AtomicBoolean(false);
        readChunk = chunkSize > 1 && reader.supportReadChunk();
        bufferSize = Math.max(10, chunkSize);


        processJobQueue = new LinkedList<>();
        writeJobQueue = new LinkedList<>();

        if (parallelRead || parallelWrite) {
            executor = new ThreadPoolExecutor(
                    poolSize, poolSize,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new BasicThreadFactory.Builder().namingPattern("sb-pool-%d").build());
        }
        if (parallelRead) {
            readQueue = new LinkedBlockingQueue<>();
            readJobQueue = new LinkedBlockingQueue<>();
            handleManager = new HandleManager();
        }

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
        if (null != handleManager && handleManager.isAlive()) {
            handleManager.interrupt();
        }
        if (null != executor) {
            // executor.shutdownNow();
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

        // clear queue
        Optional.ofNullable(readJobQueue).ifPresent(Queue::clear);
        Optional.ofNullable(readQueue).ifPresent(Queue::clear);

        // close reader、writer、processor
        closeReader();
        closeWriter();
        closeProcessor();

        started = false;
        LOGGER.info("Execution {}, {}", aborted.get() ? "failed" : "succeeded", metrics.toString());
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

    // private void readRecords() {
    //     if (parallelRead) {
    //         for (int i = 0; i <= poolSize; i++) {
    //             readJobQueue.offer(executor.submit(new RecordReadJob()));
    //         }
    //     } else {
    //         while (true) {
    //             if (aborted.get()) {
    //                 return;
    //             }
    //             Collection<I> records = doRead();
    //             if (null != records) {
    //                 for (I record : records) {
    //                     readQueue.offer(record);
    //                 }
    //             } else {
    //                 break;
    //             }
    //         }
    //     }
    // }

    private void processRecord(I record) {
        if (parallelProcess) {
            processJobQueue.offer(executor.submit(new RecordProcessJob(record)));
        } else {
            doProcess(record);
        }
    }

    private void writeRecords(List<O> records) {
        if (parallelWrite) {
            executor.submit(new RecordWriteJob(records));
        } else {
            doWrite(records);
        }
    }


    private void processAndWriteRecords(List<I> buffer) {
        if (parallelWrite) {
            executor.submit(new RecordHandlingJob(buffer));
        } else {
            processAndWrite(buffer);
        }
    }

    private void processAndWrite(List<I> buffer) {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        // notice that if some records are filtered, the chunk to be written will be smaller
        List<O> data = new ArrayList<>(buffer.size());
        for (I ea : buffer) {
            try {
                O o = processor.processRecord(ea);
                if (null == o) {
                    metrics.incrementFilterCount();
                } else {
                    data.add(o);
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

        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        try {
            writer.write(data);
            metrics.addWriteCount(data.size());
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("Exception in writing record", e);
                metrics.addErrorCount(data.size());
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("Error occurred while stop writing forcibly.", e);
            }
        }
    }

    private void waitForJobs(Queue<Future<Boolean>> queue) throws InterruptedException {
        Future<Boolean> future;
        while ((future = queue.poll()) != null) {
            try {
                future.get();
            } catch (ExecutionException e) {
                LOGGER.error("Unexpected error while waiting job complete.", e);
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

    private final class HandleManager extends Thread {

        HandleManager() {
            super("handle-manager");
        }

        @Override
        public void run() {
            LOGGER.info("HandleManager started ...");
            try {
                I record;
                List<I> buffer = new ArrayList<>(bufferSize);
                while (!readFinished) {
                    record = readQueue.poll(QUEUE_WAIT, TimeUnit.MILLISECONDS);
                    if (aborted.get()) {
                        return;
                    }
                    if (null != record) {
                        buffer.add(record);
                        if (buffer.size() == chunkSize) {
                            processAndWriteRecords(buffer);
                            buffer = new ArrayList<>(bufferSize);
                        }
                    }
                }

                if (aborted.get()) {
                    return;
                }

                // flush
                while (true) {
                    if (buffer.size() == chunkSize) {
                        processAndWriteRecords(buffer);
                        buffer = new ArrayList<>(bufferSize);
                    }

                    if (aborted.get()) {
                        return;
                    }

                    record = readQueue.poll();
                    if (null != record) {
                        buffer.add(record);
                    } else {
                        if (!buffer.isEmpty()) {
                            processAndWriteRecords(buffer);
                        }
                        break;
                    }
                }
            } catch (InterruptedException | BatchAbortedException ignore) {
                // InterruptedException: only main thread will interrupt manager
                // BatchAbortedException: parallel read and order write, writer stopped by reader, just return
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while processing or writing records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("HandleManager stopped.");
            }
        }
    }

    private class RecordProcessJob implements Callable<Boolean> {

        I record;

        public RecordProcessJob(I record) {
            this.record = record;
        }

        @Override
        public Boolean call() throws Exception {
            if (aborted.get()) {
                return false;
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
            return true;
        }
    }

    private class RecordWriteJob implements Callable<Boolean> {

        List<O> records;

        public RecordWriteJob(List<O> records) {
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            if (aborted.get()) {
                return false;
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
            return true;
        }
    }

    private class RecordHandlingJob implements Callable<Boolean> {

        List<I> records;

        RecordHandlingJob(List<I> records) {
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            if (aborted.get()) {
                return false;
            }
            try {
                processAndWrite(records);
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("Exceed failover, abort execution.");
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("Unexpected error happened while processing or writing records, abort execution.", e);
                aborted.set(true);
                mainThread.interrupt();
            }
            return true;
        }
    }

    private class RecordReadJob implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            if (readOver || aborted.get()) {
                return false;
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
            return true;
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
