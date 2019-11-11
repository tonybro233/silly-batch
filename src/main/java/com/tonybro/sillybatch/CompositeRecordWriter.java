package com.tonybro.sillybatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeRecordWriter <T> implements RecordWriter<T> {

    private List<RecordWriter<T>> writers;

    public CompositeRecordWriter() {
        this(new ArrayList<>());
    }

    public CompositeRecordWriter(List<RecordWriter<T>> writers) {
        this.writers = writers;
    }

    public void addWriter(RecordWriter<T> writer) {
        writers.add(writer);
    }

    public RecordWriter<T> getWriter(int idx) {
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
        for (RecordWriter<T> writer : writers) {
            writer.write(record);
        }
    }

    @Override
    public void write(Collection<T> records) throws Exception {
        for (RecordWriter<T> writer : writers) {
            writer.write(records);
        }
    }
}
