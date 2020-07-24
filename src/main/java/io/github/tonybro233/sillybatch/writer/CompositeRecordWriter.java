package io.github.tonybro233.sillybatch.writer;

import java.util.ArrayList;
import java.util.List;

public class CompositeRecordWriter <T> implements RecordWriter<T> {

    private List<RecordWriter<? super T>> writers = new ArrayList<>();

    public CompositeRecordWriter() {
    }

    public CompositeRecordWriter(List<? extends RecordWriter<? super T>> writers) {
        this.writers.addAll(writers);
    }

    public void addWriter(RecordWriter<? super T> writer) {
        writers.add(writer);
    }

    public RecordWriter<? super T> getWriter(int idx) {
        return writers.get(idx);
    }

    public int size() {
        return this.writers.size();
    }

    @Override
    public void open() throws Exception {
        for (RecordWriter writer : writers) {
            writer.open();
        }
    }

    @Override
    public void close() throws Exception {
        for (RecordWriter writer : writers) {
            writer.close();
        }
    }

    @Override
    public void write(T record) throws Exception {
        for (RecordWriter<? super T> writer : writers) {
            writer.write(record);
        }
    }
}
