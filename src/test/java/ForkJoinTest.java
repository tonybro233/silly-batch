import com.tonybro.sillybatch.RecordProcessor;
import com.tonybro.sillybatch.RecordReader;
import com.tonybro.sillybatch.RecordWriter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ForkJoinTest {

    private LinkedBlockingDeque<Integer> queue = new LinkedBlockingDeque<>();

    private RecordReader<Integer> reader;

    private RecordProcessor<Integer, Integer> processor;

    private RecordWriter<Integer> writer;

    @Test
    public void test() throws ExecutionException, InterruptedException {
        ForkJoinPool pool = new ForkJoinPool(32);
        ForkJoinTask<Void> readFuture = pool.submit(new ReadTask());
        ForkJoinTask<Void> writeManageFuture = pool.submit(new WriteMangeTask());
        readFuture.get();
        writeManageFuture.get();

        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);
    }

    public class ReadTask extends RecursiveAction {

        @Override
        protected void compute() {
            try {
                Integer record;
                while ((record = reader.read()) != null) {
                    ProcessTask processTask = new ProcessTask(record);
                    processTask.fork();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public class ProcessTask extends RecursiveAction {

        private Integer record;

        public ProcessTask(Integer record) {
            this.record = record;
        }

        @Override
        protected void compute() {
            try {
                Integer out = processor.processRecord(record);
                queue.put(out);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public class WriteMangeTask extends RecursiveAction {

        @Override
        protected void compute() {
            try {
                Integer record;
                List<Integer> buffer = new ArrayList<>();
                while (true) {
                    record = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (null != record) {
                        buffer.add(record);
                        if (buffer.size() == 5) {
                            new WriteTask(buffer).fork();
                            buffer = new ArrayList<>();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public class WriteTask extends RecursiveAction {

        private List<Integer> records;

        public WriteTask(List<Integer> records) {
            this.records = records;
        }

        @Override
        protected void compute() {
            try {
                writer.write(records);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
