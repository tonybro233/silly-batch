package com.tonybro.sillybatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeRecordReader<T> implements RecordReader<T> {

    private List<RecordReader<T>> readers;

    private volatile int currentIdx;

    private boolean strict = false;

    public CompositeRecordReader() {
        this(new ArrayList<>());
    }

    public CompositeRecordReader(List<RecordReader<T>> readers) {
        this.readers = readers;
    }

    public void addReader(RecordReader<T> reader) {
        readers.add(reader);
    }

    public RecordReader<T> getReader(int idx) {
        return readers.get(idx);
    }

    public int size() {
        return this.readers.size();
    }

    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    @Override
    public void open() throws Exception {
        currentIdx = 0;
        if (readers.size() == 0) {
            if (strict) {
                throw new IllegalStateException(
                        "No readers. Set strict=false if this is not an error condition.");
            }
        } else {
            // decrease complexity, open all of them rather than open in order
            for (RecordReader reader : readers) {
                reader.open();
            }
        }
    }

    @Override
    public void close() throws Exception {
        currentIdx = readers.size();
        for (RecordReader reader : readers) {
            reader.close();
        }
    }

    @Override
    public T read() throws Exception {
        if (currentIdx >= readers.size()) {
            return null;
        }
        int idx = currentIdx;
        try {
            T record = readers.get(idx).read();
            while (null == record) {
                idx++;
                if (idx < readers.size()) {
                    record = readers.get(idx).read();
                } else {
                    break;
                }
            }
            return record;
        } finally {
            if (idx > currentIdx) {
                currentIdx = idx;
            }
        }
    }

    @Override
    public Long getTotal() throws Exception {
        long total = 0;
        for (RecordReader reader : readers) {
            Long t = reader.getTotal();
            if (t == null) {
                return null;
            } else {
                total += t;
            }
        }
        return total;
    }

    @Override
    public boolean supportReadChunk() {
        for (RecordReader reader : readers) {
            if (reader.supportReadChunk()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Collection<T> readChunk(int size) throws Exception {
        if (currentIdx >= readers.size()) {
            return null;
        }
        List<T> buffer = new ArrayList<>(size);
        int idx = currentIdx;
        try {
            while (buffer.size() < size) {
                RecordReader<T> reader = readers.get(idx);
                if (reader.supportReadChunk()) {
                    Collection<T> chunk = reader.readChunk(size - buffer.size());
                    if (null == chunk) {
                        idx++;
                        if (idx >= readers.size()) {
                            break;
                        }
                    } else {
                        buffer.addAll(chunk);
                    }
                } else {
                    T record = reader.read();
                    if (null == record) {
                        idx++;
                        if (idx >= readers.size()) {
                            break;
                        }
                    } else {
                        buffer.add(record);
                    }
                }
            }

            if (buffer.size() > 0) {
                return buffer;
            } else {
                return null;
            }
        } finally {
            if (idx > currentIdx) {
                currentIdx = idx;
            }
        }

    }
}
