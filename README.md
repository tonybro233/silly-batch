# silly-batch
“傻批”是一个简单的、追求快速处理顺序无关数据的java批处理工具。有的时候，你只是期望快速将一批数据处理完成，对输出数据的顺序并没有要求，不需要中断恢复等复杂特性，没有复杂的分支流程，而是专注于读、计算处理、写这三个核心步骤，同时你不想使用或者没有集群或者不会使用Hadoop等大数据处理框架，那么太棒了，你可以使用傻批来帮助你将原本一个上午才能跑完的数据缩短到几个小时或者几十分钟。同时你也可以利用傻批来快速进行一些并发性能测试。

### 使用方式

Maven中央仓库可用	

```xml
<dependency>
    <groupId>io.github.tonybro233</groupId>
    <artifactId>sillybatch</artifactId>
    <version>1.3</version>
</dependency>
```

### 工作原理

傻批的核心运行流程是经典的生产者/消费者模型，利用线程池和阻塞队列进行高效并发。只需要实现读取（RecordReader）、处理（RecordProcessor）和输出（RecordWriter）三个接口，然后简单配置即可开始执行需求的任务。三个核心步骤内部都可以自由选择并行或者顺序执行，使用并行模式时注意输入源、输出目标是否支持，并且组件是线程安全的即可。傻批会创建三个管理线程分别控制三个核心步骤，负责执行方法函数（顺序执行模式）或提交执行任务（并行执行模式）以及与阻塞队列交互。全部采用并行模式时，整个处理过程可简单描述为如下：

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

### 参数列表

除了提供Reader、Processor和Writer组件之外，“傻批”还额外提供了一些调控参数如下：

| 参数名称           | 类型    | 简介                         | 默认值        |
| ------------------ | ------- | ---------------------------- | ------------- |
| parallelRead       | Boolean | 是否并行读取数据             | false         |
| parallelProcess    | Boolean | 是否并行处理数据             | false         |
| parallelWrite      | Boolean | 是否并行写数据               | false         |
| forceOrder         | Boolean | 强制按顺序读、处理、写       | false         |
| failOver           | Long    | 处理失败容错次数             | 0             |
| poolSize           | Integer | 默认线程池大小               | cpu核心数 * 2 |
| readQueueCapacity  | Integer | 读取队列大小                 | 1000          |
| writeQueueCapacity | Integer | 写队列大小                   | 1000          |
| needConfirm        | Boolean | 是否需要在开始处理前进行确认 | false         |

这些参数都拥有对应的set方法，并且能够在Builder中进行指定，参数详细信息请参看builder的javadoc。

### 使用示例

这段代码将会并行读取长度为2000的随机数组100次，同时并行对这些数组执行冒泡排序，然后打印排序时间。

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
                .report(false)
                // .forceOrder(true)
                // .failover(10)
                .needConfirm(true)
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

### 注意事项

- 输出采用了SLF4J的API，可轻松集成具体的日志实现。支持通过MBean查看运行状态。
- 使用并行读时，采取的方式是提交线程池大小个递归提交的读取任务，直到`read()`方法返回null 。
- 对于每个步骤，使用并行模式时默认都会创建独立线程池，这意味着最多会创建3个默认线程池。您也可以指定外部线程池，为三个步骤指定相同的线程池（通常来说不建议这么做）。当您需要指定使用外部线程池时，请务必理解“傻批”的工作原理，调整好线程池的阻塞队列大小以及指定合适的`RejectedExecutionHandler`。
- 内部默认采用有界队列来传递数据，防止存储过多的中间数据导致内存耗尽。如果内存足够且追求极致速度，您可以将readQueue和writeQueue的大小设为`Integer.MAX_VALUE`。使用外部线程池时，您需要调整线程池的阻塞队列大小，保证其与内部队列大小适配。
- 并行意味着牺牲有序，当然“傻批”也可以顺序执行数据（默认），同时支持使用`forceOrder`选项指定Read、Process、write按顺序依次执行，即所有数据读取完毕后再处理，全部处理完毕后再写。需要注意的是在forceOrder模式下，readQueue和writeQueue将变为无界队列，所有数据将被读入内存，请保证内存足够否则将出现OOM异常。
- 在并行模式下，执行失败（超过容错上限）时可能无法立刻停止（java线程模型所限），但是会尽快。
- 小心不要过火把机子跑死。