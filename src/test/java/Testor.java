import com.tonybro.sillybatch.RecordProcessor;
import com.tonybro.sillybatch.RecordReader;
import com.tonybro.sillybatch.RecordWriter;
import com.tonybro.sillybatch.SillyBatchBuilder;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class Testor {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Testor.class);

    public static void main(String[] args) {
        SillyBatchBuilder.<Integer>newBuilder()
                .parallelRead(true)
                .parallelProcess(true)
                .parallelWrite(true)
                .forceOrder(true)
                .chunkSize(10)
                .failover(8)
                .addReader(new TestReader())
                .addReader(new TestReader())
                .addProcessor(new TestProcessor())
                .addWriter(new TestWriter())
                .build()
                .execute();
    }

    public static class TestReader implements RecordReader<Integer> {

        private AtomicInteger atomic = new AtomicInteger(0);

        @Override
        public Integer read() throws Exception {
            Thread.sleep(1000);
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
        public Integer processRecord(Integer record) throws Exception {
            if (record % 2 == 0) {
                return record * 2;
            } else {
                return null;
            }
        }
    }


    public static class TestWriter implements RecordWriter<Integer> {
        @Override
        public void write(Integer record) throws Exception {
            log.info("Write Single :" + record);
        }

        @Override
        public void write(Collection<Integer> records) throws Exception {
            // if (records.size() < 5) {
            //     throw new RuntimeException("Haha, shitter! size:" + records.size());
            // }
            log.info("Write Chunk :" + records.toString());
        }
    }
}
