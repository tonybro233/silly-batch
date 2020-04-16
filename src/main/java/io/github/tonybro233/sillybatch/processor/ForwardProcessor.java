package io.github.tonybro233.sillybatch.processor;

public class ForwardProcessor<T> implements RecordProcessor<T, T> {

    @Override
    public T process(T record) throws Exception {
        return record;
    }

}
