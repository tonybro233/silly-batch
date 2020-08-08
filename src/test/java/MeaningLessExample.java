import io.github.tonybro233.sillybatch.job.SillyBatchBuilder;
import io.github.tonybro233.sillybatch.listener.RecordReadListener;
import io.github.tonybro233.sillybatch.processor.RecordProcessor;
import io.github.tonybro233.sillybatch.reader.RecordReader;
import io.github.tonybro233.sillybatch.util.PriorityThreadPoolExecutor;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MeaningLessExample {

    public static void main(String[] args) {
        int poolsize = Runtime.getRuntime().availableProcessors() * 2;
        PriorityThreadPoolExecutor executor = new PriorityThreadPoolExecutor(
                poolsize, poolsize, 0, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<>());

        SillyBatchBuilder.<int[]>newBuilder()
                .name("Repeat bubble sort")
                .addReader(new RandomArrayReader())
                .addProcessor(new BubbleSortProcessor())
                .addWriter(d -> System.out.println("Bubble sort using time : " + d))
                .addListener(new ReadTimeListener())
                .parallelRead(executor)
                .parallelProcess(executor)
                // .chunkSize(2)
                .report(false)
                .build()
                .execute();

        executor.shutdown();
    }

    public static class RandomArrayReader implements RecordReader<int[]> {

        private AtomicInteger limit = new AtomicInteger();

        @Override
        public int[] read() throws Exception {
            if (limit.incrementAndGet() > 100) {
                return null;
            }

            // slow down
            Thread.sleep(1000);

            Random random = new Random();
            int size = 20000;
            int[] array = new int[size];
            for (int i = 0; i < size; i++) {
                array[i] = random.nextInt(size);
            }
            return array;
        }
    }

    public static class BubbleSortProcessor implements RecordProcessor<int[], Duration> {

        @Override
        public Duration process(int[] record) throws Exception {
            if (record.length < 2) {
                return null;
            }
            LocalDateTime begin = LocalDateTime.now();
            sort(record);
            LocalDateTime end = LocalDateTime.now();
            return Duration.between(begin, end);
        }

        private void sort(int[] array) {
            int N = array.length;
            for (int i = N - 1; i > 0; i--) {
                boolean noChange = true;
                for (int j = 0; j < i; j++) {
                    if (array[j] > array[j + 1]) {
                        swap(array, j, j + 1);
                        noChange = false;
                    }
                }
                if (noChange) {
                    break;
                }
            }
        }

        void swap(int[] arr, int i, int j) {
            int temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }
    }

    public static class ReadTimeListener implements RecordReadListener<int[]> {

        private ThreadLocal<Long> millis = new ThreadLocal<>();

        @Override
        public void beforeRead() {
            millis.set(System.currentTimeMillis());
        }

        @Override
        public void afterRead(int[] record) {
            long time = System.currentTimeMillis() - millis.get();
            System.out.println("Generated " + record.length
                    + " sized array using time : " + time + " millis.");
        }

        @Override
        public void onReadError(Exception ex) {
        }
    }
}
