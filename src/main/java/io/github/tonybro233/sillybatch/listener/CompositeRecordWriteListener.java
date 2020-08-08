package io.github.tonybro233.sillybatch.listener;

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
    public void beforeWrite(T record) {
        for (RecordWriteListener<? super T> listener : listeners) {
            listener.beforeWrite(record);
        }
    }

    @Override
    public void afterWrite(T record) {
        for (ListIterator<RecordWriteListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().afterWrite(record);
        }
    }

    @Override
    public void onWriteError(Exception exception, T record) {
        for (ListIterator<RecordWriteListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().onWriteError(exception, record);
        }
    }
}
