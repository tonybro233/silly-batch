package io.github.tonybro233.sillybatch.writer;

public class NullWriter implements RecordWriter<Object> {

    public static final NullWriter INSTANCE = new NullWriter();

    private NullWriter() { }

    @Override
    public void write(Object records) throws Exception { }

}
