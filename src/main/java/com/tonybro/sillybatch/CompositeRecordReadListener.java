package com.tonybro.sillybatch;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class CompositeRecordReadListener<T> implements RecordReadListener<T> {

    private List<RecordReadListener<? super T>> listeners = new ArrayList<>();

    public CompositeRecordReadListener() {
    }

    public CompositeRecordReadListener(List<? extends RecordReadListener<? super T>> listeners) {
        this.listeners.addAll(listeners);
    }

    public void addListener(RecordReadListener<? super T> listener) {
        listeners.add(listener);
    }

    public int size() {
        return this.listeners.size();
    }

    @Override
    public void beforeRead() {
        for (RecordReadListener listener : listeners) {
            listener.beforeRead();
        }
    }

    @Override
    public void afterRead(T record) {
        for (ListIterator<RecordReadListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().afterRead(record);
        }
    }

    @Override
    public void onReadError(Exception ex) {
        for (ListIterator<RecordReadListener<? super T>> iterator
             = listeners.listIterator(listeners.size()); iterator.hasPrevious();) {
            iterator.previous().onReadError(ex);
        }
    }
}
