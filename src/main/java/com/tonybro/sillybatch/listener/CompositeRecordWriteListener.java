package com.tonybro.sillybatch.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class CompositeRecordWriteListener<T> implements RecordWriteListener<T> {

    private List<RecordWriteListener<? super T>> listeners = new ArrayList<>();

    public CompositeRecordWriteListener() {
    }

    public CompositeRecordWriteListener(List<? extends RecordWriteListener<? super T>> listeners) {
        this.listeners.addAll(listeners);
    }

    public void addListener(RecordWriteListener<? super T> listener) {
        listeners.add(listener);
    }

    public int size() {
        return this.listeners.size();
    }

    @Override
    public void beforeWrite(List<? extends T> records) {
        for (RecordWriteListener<? super T> listener : listeners) {
            listener.beforeWrite(records);
        }
    }

    @Override
    public void afterWrite(List<? extends T> records) {
        for (ListIterator<RecordWriteListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().afterWrite(records);
        }
    }

    @Override
    public void onWriteError(Exception exception, List<? extends T> records) {
        for (ListIterator<RecordWriteListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().onWriteError(exception, records);
        }
    }
}
