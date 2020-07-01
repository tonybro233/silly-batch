package io.github.tonybro233.sillybatch.job;

import io.github.tonybro233.sillybatch.listener.RecordProcessListener;
import io.github.tonybro233.sillybatch.listener.RecordReadListener;
import io.github.tonybro233.sillybatch.listener.RecordWriteListener;
import io.github.tonybro233.sillybatch.log.ConsoleLogger;
import io.github.tonybro233.sillybatch.processor.RecordProcessor;
import io.github.tonybro233.sillybatch.reader.RecordReader;
import io.github.tonybro233.sillybatch.util.BasicThreadFactory;
import io.github.tonybro233.sillybatch.writer.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple and fast batch job, using producer/consumer model,
 * good at boosting work by parallel processing tasks but doesn't
 * support complex features such as job recover.
 *
 * <p>This batch has three classic steps: Read -> Process -> Write,
 * you can choose to handle records in order or in parallel inside every
 * step(default is in order). When all steps are using parallel mode,
 * the flow can be described as bellow (all steps are started at the same
 * time by default) :
 *
 * <pre>
 *             executor                             executor                              executor
 *            ╭────────╮                          ╭───────────╮                          ╭────────╮
 *            │ read() │                          │ process() │                          │ write()│
 * ╭──────╮ / │  ···   │ \   queue    ╭───────╮ / │    ···    │ \   queue    ╭───────╮ / │  ···   │
 * │source│ ─ │ read() │ ─ │=======│->│manager│ ─ │ process() │ ─ │=======│->│manager│ ─ │ write()│
 * ╰──────╯ \ │  ···   │ /            ╰───────╯ \ │    ···    │ /            ╰───────╯ \ │  ···   │
 *            │ read() │                          │ process() │                          │ write()│
 *            ╰────────╯                          ╰───────────╯                          ╰────────╯
 * </pre>
 *
 * <p>While using parallel mode, you have to ensure that handlers
 * ({@link RecordReader}, {@link RecordProcessor}, {@link RecordWriter})
 * and listeners ({@link RecordReadListener}, {@link RecordProcessListener},
 * {@link RecordWriteListener}) are thread safe, and be aware that
 * if you don't provide executor, silly batch will create executors
 * (fixed thread pool) for every step (means there are up to three
 * executors, but you are able to assign same executor for multiple steps).
 *
 * <p>Using {@link SillyBatchBuilder} to build instances.
 *
 * @author tony
 */
public class SillyBatch<I, O> {

    private static final Logger LOGGER;

    static {
        boolean hasLogger = true;
        try {
            Class.forName("org.slf4j.impl.StaticLoggerBinder");
        } catch (ClassNotFoundException ignore) {
            hasLogger = false;
        }
        LOGGER = hasLogger ? LoggerFactory.getLogger(SillyBatch.class) : new ConsoleLogger();
    }

    /* ------------------------- param -------------------------- */

    private String name = "silly-batch";

    // read record concurrently
    private boolean parallelRead = false;

    // process record concurrently
    private boolean parallelProcess = false;

    // write record concurrently
    private boolean parallelWrite = false;

    // do reading, processing, writing in order
    private boolean forceOrder = false;

    // chunk size for reading and writing
    private int chunkSize = 1;

    // threshold of error
    private long failover = 0;

    // report metrics continuously by logging
    private boolean report = true;

    // report interval
    private long reportInterval = 2000;

    // default pool size while creating executor(fixed thread pool)
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;

    // capacity of read queue, also affect internal process executor's work queue
    private int readQueueCapacity = Integer.MAX_VALUE;

    // capacity of write queue, also affect internal write executor's work queue
    private int writeQueueCapacity = Integer.MAX_VALUE;

    /* ------------------------- core -------------------------- */

    private RecordReader<? extends I> reader;

    private RecordProcessor<? super I, ? extends O> processor;

    private RecordWriter<? super O> writer;

    /* ----------------------- listener ------------------------ */

    private RecordReadListener<? super I> readListener;

    private RecordProcessListener<? super I, ? super O> processListener;

    private RecordWriteListener<? super O> writeListener;

    /* --------------------- multi thread ---------------------- */

    private ExecutorService readExecutor;

    private ExecutorService processExecutor;

    private ExecutorService writeExecutor;

    private Queue<Future<?>> readJobQueue;

    private Queue<Future<?>> processJobQueue;

    private Queue<Future<?>> writeJobQueue;

    private LinkedBlockingQueue<I> readQueue;

    private LinkedBlockingQueue<O> writeQueue;

    private CountDownLatch readOverLatch;

    /* ------------------------- mark -------------------------- */

    private boolean externalReadExecutor = false;

    private boolean externalProcessExecutor = false;

    private boolean externalWriteExecutor = false;

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

    private static final long THREAD_TIMEOUT = 5000L;

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
            // interrupter will log the exception
            aborted.set(true);
        } catch (Exception e) {
            aborted.set(true);
            LOGGER.error("({}) Error occurred.", name, e);
        } catch (Throwable e) {
            aborted.set(true);
            if (e instanceof OutOfMemoryError) {
                LOGGER.error("OOM occurred, if datasource can be treated as a stream and reader is much more faster than processor, " +
                        "you should limit the capacity of read queue(setReadQueueCapacity) to make reader slow down; " +
                        "if writer is much more slower than processor or reader, you should limit " +
                        "the capacity of write queue(setWriteQueueCapacity) to make processor slow down. " +
                        "If you limited the write queue, you should limit the read queue at the same time.");
            }
            throw e;
        } finally {
            // clean context
            teardown();
        }

        return aborted.get() ? 1 : 0;
    }

    private void prepare() throws Exception {
        throwExceptionIfStarted();
        LOGGER.info("Prepare executing {} !\n\tparallelRead={}, \n\tparallelProcess={}, \n\tparallelWrite={},"
                        + "\n\tforceOrder={}, \n\tchunk={}, \n\tfailover={}, \n\tdefault-poolSize={},"
                        + "\n\treadQueueCapacity={}, \n\twriteQueueCapacity={}",
                name, parallelRead, parallelProcess, parallelWrite,
                forceOrder, chunkSize, failover, poolSize,
                readQueueCapacity == Integer.MAX_VALUE ? "Infinite" : readQueueCapacity,
                writeQueueCapacity == Integer.MAX_VALUE ? "Infinite" : writeQueueCapacity);

        if (writeQueueCapacity != Integer.MAX_VALUE && readQueueCapacity == Integer.MAX_VALUE) {
            LOGGER.warn("You have set the write queue's capacity but not set the read queue's capacity, "
                    + "this may cause data pile up in read queue !");
        }

        if (forceOrder && (writeQueueCapacity != Integer.MAX_VALUE || readQueueCapacity != Integer.MIN_VALUE)) {
            throw new IllegalArgumentException("ForceOrder option is not compatible with limited queue capacity");
        }

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

        readQueue = new LinkedBlockingQueue<>(readQueueCapacity);
        writeQueue = new LinkedBlockingQueue<>(writeQueueCapacity);

        if (parallelRead) {
            readJobQueue = new LinkedList<>();
            if (!externalReadExecutor) {
                readExecutor = new ThreadPoolExecutor(
                        poolSize, poolSize,
                        THREAD_TIMEOUT, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new BasicThreadFactory.Builder()
                                .namingPattern("sb-reader-%d")
                                .build());
                ((ThreadPoolExecutor) readExecutor).allowCoreThreadTimeOut(true);
            }
        }
        if (parallelProcess) {
            processJobQueue = new LinkedList<>();
            if (!externalProcessExecutor) {
                processExecutor = new ThreadPoolExecutor(
                        poolSize, poolSize,
                        THREAD_TIMEOUT, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(readQueueCapacity),
                        new BasicThreadFactory.Builder()
                                .namingPattern("sb-processor-%d")
                                .priority(Thread.NORM_PRIORITY + 1)
                                .build(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
                ((ThreadPoolExecutor) processExecutor).allowCoreThreadTimeOut(true);
            }
        }
        if (parallelWrite) {
            writeJobQueue = new LinkedList<>();
            if (!externalWriteExecutor) {
                writeExecutor = new ThreadPoolExecutor(
                        poolSize, poolSize,
                        THREAD_TIMEOUT, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(writeQueueCapacity),
                        new BasicThreadFactory.Builder()
                                .namingPattern("sb-writer-%d")
                                .priority(Thread.NORM_PRIORITY + 2)
                                .build(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
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

        LOGGER.info("({}) Execution started ...", name);
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

        if (parallelRead) {
            if (externalReadExecutor) {
                interruptJobs(readJobQueue);
            } else {
                tryShutdownExecutor(readExecutor, "readExecutor");
            }
        }

        if (parallelProcess) {
            if (externalProcessExecutor) {
                interruptJobs(processJobQueue);
            } else {
                tryShutdownExecutor(processExecutor, "processExecutor");
            }
        }

        if (parallelWrite) {
            if (externalWriteExecutor) {
                interruptJobs(writeJobQueue);
            } else {
                tryShutdownExecutor(writeExecutor, "writeExecutor");
            }
        }

        if (parallelRead && !externalReadExecutor && !readExecutor.isTerminated()) {
            LOGGER.info("({}) Wait for readExecutor to be terminated timeout, into force clean mode ...", name);
            forceClean = true;
            readExecutor.shutdownNow();
        }

        if (parallelProcess && !externalProcessExecutor && !processExecutor.isTerminated()) {
            LOGGER.info("({}) Wait for processExecutor to be terminated timeout, into force clean mode ...", name);
            forceClean = true;
            processExecutor.shutdownNow();
        }

        if (parallelWrite && !externalWriteExecutor && !writeExecutor.isTerminated()) {
            LOGGER.info("({}) Wait for writeExecutor to be terminated timeout, into force clean mode ...", name);
            forceClean = true;
            writeExecutor.shutdownNow();
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
        LOGGER.info("({}) Execution {}!\n{}", name, aborted.get() ? "failed" : "succeeded", metrics.toString());
    }

    private void throwExceptionIfStarted() {
        if (started) {
            throw new IllegalStateException("This batch instance has already started!");
        }
    }

    private void tryShutdownExecutor(ExecutorService executor, String desc) {
        if (!executor.isShutdown()) {
            LOGGER.info("({}) Terminating {} ...", name, desc);
            executor.shutdown();
        }
        try {
            executor.awaitTermination(SHUTDOWN_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("({}) Wait for {} to be terminated interrupted.", name, desc);
        }
    }

    private List<? extends I> doRead() {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }
        try {
            if (null != readListener) { readListener.beforeRead(); }
            List<? extends I> records = null;
            if (readChunk) {
                records = reader.readChunk(chunkSize);
                if (null != records) {
                    if (null != readListener) { readListener.afterRead(records); }
                    metrics.addReadCount(records.size());
                }
            } else {
                I record = reader.read();
                if (null != record) {
                    if (null != readListener) { readListener.afterRead(record); }
                    metrics.incrementReadCount();
                    records = Collections.singletonList(record);
                }
            }

            return records;
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("({}) Error reading records", name, e);
                onReadError(e);
                metrics.addErrorCount(readChunk ? chunkSize : 1);
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("({}) Error occurred while stop reading forcibly.", name, e);
            }
            return Collections.emptyList();
        }
    }

    private void doProcess(I record) {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        try {
            if (null != processListener) { processListener.beforeProcess(record); }
            O out = processor.process(record);
            if (null != processListener) { processListener.afterProcess(record, out); }
            if (null == out) {
                metrics.incrementFilterCount();
            } else {
                metrics.incrementProcessCount();
                while (!writeQueue.offer(out, QUEUE_WAIT, TimeUnit.MILLISECONDS)) {
                    if (aborted.get()) {
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            if (aborted.get()) {
                LOGGER.trace("({}) Sending data to write queue interrupted", name, e);
            } else {
                throw new RuntimeException("Interrupted while waiting to send data to write queue", e);
            }
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("({}) Exception in processing record", name, e);
                onProcessError(e, record);
                metrics.incrementErrorCount();
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("({}) Error occurred while stop processing forcibly.", name, e);
            }
        }
    }

    private void doWrite(List<O> records) {
        if (aborted.get()) {
            throw new BatchAbortedException();
        }

        try {
            if (null != writeListener) { writeListener.beforeWrite(records); }
            writer.write(records);
            metrics.addWriteCount(records.size());
            if (null != writeListener) { writeListener.afterWrite(records); }
        } catch (Exception e) {
            if (!forceClean) {
                LOGGER.error("({}) Exception in writing records", name, e);
                onWriteError(e, records);
                metrics.addErrorCount(records.size());
                if (metrics.getErrorCount() > failover) {
                    throw new FailOverExceededException();
                }
            } else {
                LOGGER.error("({}) Error occurred while stop writing forcibly.", name, e);
            }
        }
    }

    private void processRecord(I record) {
        if (parallelProcess) {
            // maybe caller run
            processJobQueue.offer(processExecutor.submit(new RecordProcessJob(record)));
        } else {
            doProcess(record);
        }
    }

    private void writeRecords(List<O> records) {
        if (parallelWrite) {
            // maybe caller run
            writeJobQueue.offer(writeExecutor.submit(new RecordWriteJob(records)));
        } else {
            doWrite(records);
        }
    }

    private void onReadError(Exception e) {
        try {
            if (null != readListener) {
                readListener.onReadError(e);
            }
        } catch (Exception ex) {
            LOGGER.error("({}) Read listener error.", name, ex);
        }
    }

    private void onProcessError(Exception e, I record) {
        try {
            if (null != processListener) {
                processListener.onProcessError(e, record);
            }
        } catch (Exception ex) {
            LOGGER.error("({}) Process listener error.", name, ex);
        }
    }

    private void onWriteError(Exception e, List<O> records) {
        try {
            if (null != writeListener) {
                writeListener.onWriteError(e, records);
            }
        } catch (Exception ex) {
            LOGGER.error("({}) Read listener error.", name, ex);
        }
    }

    private void waitForJobs(Queue<Future<?>> queue) throws InterruptedException {
        if (null != queue) {
            Future<?> future;
            while ((future = queue.poll()) != null) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    LOGGER.error("({}) Unexpected error while waiting for job to be completed.", name, e);
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
        FailOverExceededException() {
            super("Exceed failover, abort execution.");
        }
    }

    private static class BatchAbortedException extends RuntimeException {
        BatchAbortedException() {
            super("Batch has been aborted.");
        }
    }

    /* ------------------------- thread & runnable ------------------------- */

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
                        LOGGER.info("({}) status:{}", name, metrics.report());
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
            LOGGER.info("({}) Read manager started ...", name);
            try {
                if (parallelRead) {
                    for (int i = 0; i <= poolSize; i++) {
                        readJobQueue.offer(readExecutor.submit(new RecordReadJob()));
                    }
                    readOverLatch.await();
                    waitForJobs(readJobQueue);
                } else {
                    while (true) {
                        if (aborted.get()) {
                            return;
                        }
                        List<? extends I> records = doRead();
                        if (null != records) {
                            for (I record : records) {
                                while (!readQueue.offer(record, QUEUE_WAIT, TimeUnit.MILLISECONDS)) {
                                    if (aborted.get()) {
                                        return;
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                // Under normal circumstances, only main thread will interrupt manager
                if (aborted.get()) {
                    LOGGER.trace("({}) Read manager is interrupted", name, e);
                } else {
                    throw new RuntimeException("Interrupted while waiting to send data to read queue", e);
                }
            } catch (BatchAbortedException ignore) {
                // stopped by reader or writer, just return
                LOGGER.trace("({}) Read manager: Execution aborted", name);
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while reading records, abort execution.", name, e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("({}) Read manager stopped.", name);
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
            LOGGER.info("({}) Process manager started ...", name);
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
            } catch (InterruptedException e) {
                // Under normal circumstances, only main thread will interrupt manager
                LOGGER.trace("({}) Process manager is interrupted", name, e);
            } catch (BatchAbortedException ignore) {
                // stopped by reader or writer, just return
                LOGGER.trace("({}) Process manager: Execution aborted", name);
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while processing records, abort execution.", name, e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("({}) Process manager stopped.", name);
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
            LOGGER.info("({}) Write manager started ...", name);
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
            } catch (InterruptedException e) {
                // Under normal circumstances, only main thread will interrupt manager
                LOGGER.trace("({}) Write manager is interrupted", name, e);
            } catch (BatchAbortedException ignore) {
                // stopped by reader or writer, just return
                LOGGER.trace("({}) Write manager: Execution aborted", name);
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while writing records, abort execution.", name, e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                LOGGER.info("({}) Write manager stopped.", name);
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
                List<? extends I> records = doRead();
                if (null != records) {
                    for (I record : records) {
                        while (!readQueue.offer(record, QUEUE_WAIT, TimeUnit.MILLISECONDS)) {
                            if (aborted.get()) {
                                return;
                            }
                        }
                    }
                } else {
                    readOver = true;
                    readOverLatch.countDown();
                }
            } catch (FailOverExceededException e) {
                // prevent repeat LOGGER
                if (aborted.compareAndSet(false, true)) {
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (InterruptedException e) {
                if (aborted.get()) {
                    LOGGER.trace("({}) Read job interrupted", name, e);
                } else {
                    LOGGER.error("({}) Read job interrupted (waiting to send data to read queue) while batch is still running", name, e);
                    aborted.set(true);
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while reading records, abort execution.", name, e);
                aborted.set(true);
                mainThread.interrupt();
            } finally {
                if (!readOver && !aborted.get()) {
                    readJobQueue.offer(readExecutor.submit(new RecordReadJob()));
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
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while processing record, abort execution.", name, e);
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
                    LOGGER.error("({}) Exceed failover, abort execution.", name);
                    mainThread.interrupt();
                }
            } catch (BatchAbortedException ignore) {
            } catch (Exception e) {
                LOGGER.error("({}) Unexpected error happened while writing record, abort execution.", name, e);
                aborted.set(true);
                mainThread.interrupt();
            }
        }
    }

    /* --------------------- open & close ---------------------- */

    private void openReader() throws Exception {
        LOGGER.info("({}) Opening record reader ...", name);
        try {
            reader.open();
        } catch (Exception e) {
            LOGGER.error("({}) Unable to open record reader", name, e);
            throw new Exception("Shutdown");
        }
    }

    private void openWriter() throws Exception {
        LOGGER.info("({}) Opening record writer ...", name);
        try {
            writer.open();
        } catch (Exception e) {
            LOGGER.error("({}) Unable to open record writer", name, e);
            throw new Exception("Shutdown");
        }
    }

    private void openProcessor() throws Exception {
        if (null != processor) {
            LOGGER.info("({}) Opening record processor ...", name);
            try {
                processor.open();
            } catch (Exception e) {
                LOGGER.error("({}) Unable to open record processor", name, e);
                throw new Exception("Shutdown");
            }
        }
    }

    private void closeReader() {
        try {
            LOGGER.info("({}) Closing record reader ...", name);
            reader.close();
        } catch (Exception e) {
            LOGGER.error("({}) Unable to close record reader", name, e);
        }
    }

    private void closeWriter() {
        try {
            LOGGER.info("({}) Closing record writer ...", name);
            writer.close();
        } catch (Exception e) {
            LOGGER.error("({}) Unable to close record writer", name, e);
        }
    }

    private void closeProcessor() {
        if (null != processor) {
            try {
                LOGGER.info("({}) Closing record processor ...", name);
                processor.close();
            } catch (Exception e) {
                LOGGER.error("({}) Unable to close record processor", name, e);
            }
        }
    }

    /* --------------------- setter ---------------------- */

    public void setName(String name) {
        throwExceptionIfStarted();
        this.name = name;
    }

    public void setReader(RecordReader<? extends I> reader) {
        throwExceptionIfStarted();
        this.reader = reader;
    }

    public void setProcessor(RecordProcessor<? super I, ? extends O> processor) {
        throwExceptionIfStarted();
        this.processor = processor;
    }

    public void setWriter(RecordWriter<? super O> writer) {
        throwExceptionIfStarted();
        this.writer = writer;
    }

    public void setListener(RecordReadListener<? super I> listener) {
        this.readListener = listener;
    }

    public void setListener(RecordProcessListener<? super I, ? super O> listener) {
        this.processListener = listener;
    }

    public void setListener(RecordWriteListener<? super O> listener) {
        this.writeListener = listener;
    }

    public void setParallelRead(boolean parallelRead) {
        throwExceptionIfStarted();
        this.parallelRead = parallelRead;
    }

    public void setParallelRead(ExecutorService executor) {
        throwExceptionIfStarted();
        this.parallelRead = true;
        this.readExecutor = executor;
        this.externalReadExecutor = true;
    }

    public void setParallelProcess(boolean parallelProcess) {
        throwExceptionIfStarted();
        this.parallelProcess = parallelProcess;
    }

    public void setParallelProcess(ExecutorService executor) {
        throwExceptionIfStarted();
        this.parallelProcess = true;
        this.processExecutor = executor;
        this.externalProcessExecutor = true;
    }

    public void setParallelWrite(boolean parallelWrite) {
        throwExceptionIfStarted();
        this.parallelWrite = parallelWrite;
    }

    public void setParallelWrite(ExecutorService executor) {
        throwExceptionIfStarted();
        this.parallelWrite = true;
        this.writeExecutor = executor;
        this.externalWriteExecutor = true;
    }

    public void setForceOrder(boolean ordered) {
        throwExceptionIfStarted();
        this.forceOrder = ordered;
    }

    public void setChunkSize(int chunkSize) {
        throwExceptionIfStarted();
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("ChunkSize must be positive!");
        }
        this.chunkSize = chunkSize;
    }

    public void setFailover(long failover) {
        throwExceptionIfStarted();
        this.failover = failover;
    }

    public void setPoolSize(int poolSize) {
        throwExceptionIfStarted();
        if (poolSize <= 0) {
            throw new IllegalArgumentException("PoolSize must be positive!");
        }
        this.poolSize = poolSize;
    }

    public void setReadQueueCapacity(int capacity) {
        throwExceptionIfStarted();
        if (capacity <= 0) {
            throw new IllegalArgumentException("Read queue capacity must be positive!");
        }
        this.readQueueCapacity = capacity;
    }

    public void setWriteQueueCapacity(int capacity) {
        throwExceptionIfStarted();
        if (capacity <= 0) {
            throw new IllegalArgumentException("Write queue capacity must be positive!");
        }
        this.writeQueueCapacity = capacity;
    }

    public void setReport(boolean report) {
        throwExceptionIfStarted();
        this.report = report;
    }

    public void setReportInterval(long reportInterval) {
        if (reportInterval <= 0) {
            throw new IllegalArgumentException("Interval must be positive");
        }
        this.reportInterval = reportInterval;
    }
}