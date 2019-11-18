package io.github.tonybro233.sillybatch.reader;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CollectionReader<T> implements RecordReader<T> {

    private Collection<T> collection;

    private Iterator<T> iterator;

    public CollectionReader(Collection<T> collection) {
        this.collection = collection;
    }

    @Override
    public void open() throws Exception {
        this.iterator = collection.iterator();
    }

    @Override
    public Long getTotal() throws Exception {
        return (long) collection.size();
    }

    @Override
    public T read() throws Exception {
        try {
            return iterator.next();
        } catch (NoSuchElementException ignore) {
            return null;
        }
    }
}
