package com.tonybro.sillybatch;

import java.util.ArrayList;
import java.util.List;

public class CompositeRecordProcessor <I, O> implements RecordProcessor<I, O> {

    private List<RecordProcessor<?, ?>> processors = new ArrayList<>();

    public CompositeRecordProcessor() {
    }

    public CompositeRecordProcessor(List<RecordProcessor<?, ?>> processors) {
        this.processors = processors;
    }

    public CompositeRecordProcessor(RecordProcessor<I, O> processor) {
        processors.add(processor);
    }

    public int size() {
        return this.processors.size();
    }

    @SuppressWarnings("unchecked")
    public <K> CompositeRecordProcessor<I, K> addProcessor(RecordProcessor<? super O, K> processor) {
        this.processors.add(processor);
        return (CompositeRecordProcessor<I, K>) this;
    }

    @Override
    public void open() throws Exception {
        for (RecordProcessor processor : processors) {
            processor.open();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public O process(I record) throws Exception {
        Object out = record;
        for (RecordProcessor processor : processors) {
            out = processor.process(out);
            if (out == null) {
                return null;
            }
        }
        return (O) out;
    }

    @Override
    public void close() throws Exception {
        for (RecordProcessor processor : processors) {
            processor.close();
        }
    }
}
