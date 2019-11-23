# silly-batch
Silly Batch is a simple tool for java that seek for process order-independent data quickly. Sometimes you just want handle batch of data quickly, the order of the output is not in consider and you don't need some complex features like recover execution, the work flow is simple and the most important things are three core steps: reading, processing and writing. Coincidentally you don't want to use some big data frameworks like Hadoop or you don't have clusters at the same time, then you can use Silly Batch to help you accelerate your jobs. You can also use Silly Batch to do some concurrent performance tests quickly.

### Setup

Available from Maven Central.

```xml
<dependency>
    <groupId>io.github.tonybro233</groupId>
    <artifactId>sillybatch</artifactId>
    <version>1.1</version>
</dependency>
```

### Hot it works

Silly Batch is build on the classical producer/consumer model, doing things concurrently and efficiently by using thread pool and blocking queue. Before start executing your job, you just have to implement `RecordReader`, `RecordProcessor`  and `RecordWriter` three interfaces then do some simple configuration. All three core steps are free to choose parallel or sequential execution. When using parallel mode you should guarantee the input sources or output targets support multi thread and the components are thread safe. Silly Batch will create three administrative threads to control each of steps, handle work(sequential mode) or submit job(parallel mode) and communicate with blocking queues. When all steps are using parallel mode, the whole procedure can be simply described as below: 

```
            executor                             executor                              executor
           ╭────────╮                          ╭───────────╮                          ╭────────╮
           │ read() │                          │ process() │                          │ write()│
╭──────╮ / │  ···   │ \   queue    ╭───────╮ / │    ···    │ \   queue    ╭───────╮ / │  ···   │
│source│ ─ │ read() │ ─ │=======│->│manager│ ─ │ process() │ ─ │=======│->│manager│ ─ │ write()│
╰──────╯ \ │  ···   │ /            ╰───────╯ \ │    ···    │ /            ╰───────╯ \ │  ···   │
           │ read() │                          │ process() │                          │ write()│
           ╰────────╯                          ╰───────────╯                          ╰────────╯
```

### Example

The following code will read 2000 sized random arrays of 100 times in parallel, perform bubble sort on these arrays in parallel at the same time, and then print the time used by each sorting.

``` java
public class MeaningLessExample {

    public static void main(String[] args) {
        SillyBatchBuilder.<int[]>newBuilder()
                .name("Repeat bubble sort")
                .addReader(new RandomArrayReader())
                .addProcessor(new BubbleSortProcessor())
                .addWriter(records -> {
                    for (Duration duration : records) {
                        System.out.println("Bubble sort using time : " + duration);
                    }
                })
                .addListener(new ReadTimeListener())
                .parallelRead(true)
                .parallelProcess(true)
                // .chunkSize(2)
                .report(false)
                // .forceOrder(true)
                // .failover(10)
                .build()
                .execute();
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
```

### Notes

- Using SLF4J logging api，import log implementation when using this tool.
- The way to achieve parallel reads is to submit self-submit reading job to the thread pool, the initial number of job is thread pool's size.
- For each steps, Silly Batch will create thread pool when using parallel mode, it means that there are up to three default thread pools. Of course you can provide external thread pools, even specify one thread pool for three steps. But note that Silly Batch will always trying to submit a lot of jobs to the thread pool, when using single thread pool, if the speed of reading is much more faster than others, lots of processing jobs will be submitted at once, that cause the writing jobs ranked at the tail of the thread pool's  job queue, middle data will be stacked in queue for a long time. If it is a problem, consider using `PriorityThreadPoolExecutor` with `PriorityBlockingQueue`. 
- Parallelism means sacrificing order, Silly Batch can also handle data in order(by default)，and support using `forceOrder` option to force doing processing after read over, doing writing after process over.
- When in parallel mode, job may not stopped immediately (limited by java thread model) when failed(exceed fail over), but will as soon as possible.
- Be careful not to make your computer crash down.