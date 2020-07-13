import io.github.tonybro233.sillybatch.processor.RecordProcessor;
import io.github.tonybro233.sillybatch.reader.RecordReader;
import io.github.tonybro233.sillybatch.writer.RecordWriter;
import io.github.tonybro233.sillybatch.job.SillyBatchBuilder;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Testor {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Testor.class);

    public static void main(String[] args) {
        int res = SillyBatchBuilder.<Integer>newBuilder()
                .parallelRead(false)
                .parallelProcess(true)
                .parallelWrite(true)
                // .forceOrder(true)
                .chunkSize(10)
                .failover(8)
                .readQueueCapacity(5)
                // .writeQueueCapacity(5)
                // add reader twice
                .addReader(new TestReader())
                .addReader(new TestReader())
                .addProcessor(new TestProcessor())
                .addWriter(new TestWriter())
                .needConfirm(true)
                .build()
                .execute();

        System.out.println("Return value is " + res);
    }

    public static class TestReader implements RecordReader<Integer> {

        private AtomicInteger atomic = new AtomicInteger(0);

        @Override
        public Integer read() throws Exception {
            // Thread.sleep(1000);
            int i = atomic.incrementAndGet();
            if (i >= 60 && i <= 62) {
                throw new RuntimeException("Haha, mother fucker! " + i);
            }

            if (i >= 90 && i <= 91) {
                throw new RuntimeException("Haha, mother fucker! " + i);
            }
            if (i > 100) {
                return null;
            }
            return i;
        }
    }

    public static class TestProcessor implements RecordProcessor<Integer, Integer> {
        @Override
        public Integer process(Integer record) throws Exception {
            Thread.sleep(1000);
            if (record % 2 == 0) {
                return record;
            } else {
                return null;
            }
        }
    }


    public static class TestWriter implements RecordWriter<Integer> {

        @Override
        public void write(List<? extends Integer> records) throws Exception {
            // if (records.size() < 5) {
            //     throw new RuntimeException("Haha, shitter! size:" + records.size());
            // }
            log.info("Write Chunk :" + records.toString());
        }
    }
}
