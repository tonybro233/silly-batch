package com.tonybro.sillybatch.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class CompositeRecordProcessListener<I, O> implements RecordProcessListener<I, O> {

    private List<RecordProcessListener<? super I, ? super O>> listeners = new ArrayList<>();

    public CompositeRecordProcessListener() {
    }

    public CompositeRecordProcessListener(List<? extends RecordProcessListener<? super I, ? super O>> listeners) {
        this.listeners.addAll(listeners);
    }

    public void addListener(RecordProcessListener<? super I, ? super O> listener) {
        listeners.add(listener);
    }

    public int size() {
        return this.listeners.size();
    }

    @Override
    public void beforeProcess(I record) {
        for (RecordProcessListener<? super I, ? super O> listener : listeners) {
            listener.beforeProcess(record);
        }
    }

    @Override
    public void afterProcess(I record, O result) {
        for (ListIterator<RecordProcessListener<? super I, ? super O>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().afterProcess(record, result);
        }
    }

    @Override
    public void onProcessError(Exception e, I record) {
        for (ListIterator<RecordProcessListener<? super I, ? super O>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().onProcessError(e, record);
        }
    }
}
