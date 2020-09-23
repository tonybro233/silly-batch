package io.github.tonybro233.sillybatch.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompositeRecordReader<T> implements RecordReader<T> {

    private final List<RecordReader<? extends T>> readers = new ArrayList<>();

    private volatile int currentIdx;

    private boolean strict = true;

    public CompositeRecordReader() {
    }

    public CompositeRecordReader(List<? extends RecordReader<? extends T>> readers) {
        this.readers.addAll(readers);
    }

    public void addReader(RecordReader<? extends T> reader) {
        readers.add(reader);
    }

    public RecordReader<? extends T> getReader(int idx) {
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
            for (RecordReader<? extends T> reader : readers) {
                reader.open();
            }
        }
    }

    @Override
    public void close() throws Exception {
        currentIdx = readers.size();
        for (RecordReader<? extends T> reader : readers) {
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
        for (RecordReader<? extends T> reader : readers) {
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
        for (RecordReader<? extends T> reader : readers) {
            if (reader.supportReadChunk()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<T> readChunk() throws Exception {
        if (currentIdx >= readers.size()) {
            return null;
        }
        int idx = currentIdx;
        try {
            List<T> records = adaptiveRead(idx);
            while (null == records) {
                idx++;
                if (idx < readers.size()) {
                    records = adaptiveRead(idx);
                } else {
                    break;
                }
            }
            return records;
        } finally {
            if (idx > currentIdx) {
                currentIdx = idx;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<T> adaptiveRead(int idx) throws Exception {
        return readers.get(idx).supportReadChunk() ?
                (List<T>) readers.get(idx).readChunk() :
                Collections.singletonList(readers.get(idx).read());
    }

}
