package io.github.tonybro233.sillybatch.writer;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;

public class TextLineWriter<T> implements RecordWriter<T> {

    private String filePath;

    private boolean append = false;

    private Charset charset = Charset.defaultCharset();

    private Function<T, String> stringer = Object::toString;

    private BufferedWriter writer;

    public TextLineWriter(String filePath) {
        this.filePath = filePath;
    }

    public TextLineWriter(String filePath, Function<T, String> stringer) {
        this.filePath = filePath;
        this.stringer = stringer;
    }

    public TextLineWriter(String filePath, boolean append, Charset charset, Function<T, String> stringer) {
        this.filePath = filePath;
        this.append = append;
        this.charset = charset;
        this.stringer = stringer;
    }

    @Override
    public void open() throws Exception {
        File file = new File(filePath);
        if (!append && file.exists()) {
            throw new IllegalArgumentException("The output file already exist! Path: " + filePath);
        }
        if (!Files.isWritable(file.toPath())) {
            throw new IllegalArgumentException("Unable to write file! Path: " + filePath);
        }
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, append), charset));
    }

    @Override
    public void close() throws Exception {
        if (null != writer) {
            writer.close();
        }
    }

    @Override
    public void write(List<? extends T> records) throws Exception {
        for (T record : records) {
            writer.write(stringer.apply(record));
            writer.newLine();
        }
    }
}
