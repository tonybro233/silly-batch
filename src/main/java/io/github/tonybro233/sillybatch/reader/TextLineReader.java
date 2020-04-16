package io.github.tonybro233.sillybatch.reader;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;

public class TextLineReader implements RecordReader<String> {

    private String filePath;

    private Charset charset = Charset.defaultCharset();

    private BufferedReader reader;

    public TextLineReader(String filePath) {
        this.filePath = filePath;
    }

    public TextLineReader(String filePath, Charset charset) {
        this.filePath = filePath;
        this.charset = charset;
    }

    @Override
    public void open() throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("Input file does not exist! Path: " + filePath);
        }
        if (!file.isFile()) {
            throw new IllegalArgumentException("The input path should be a file! Path: " + filePath);
        }
        if (Files.isReadable(file.toPath())) {
            throw new IllegalArgumentException("Unable to read file! Path: " + filePath);
        }
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public String read() throws Exception {
        return reader.readLine();
    }
}
