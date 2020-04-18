package io.github.tonybro233.sillybatch.writer;

import java.util.List;

public class NullWriter implements RecordWriter<Object> {

    public static final NullWriter INSTANCE = new NullWriter();

    private NullWriter() { }

    @Override
    public void write(List<?> records) throws Exception { }

}
