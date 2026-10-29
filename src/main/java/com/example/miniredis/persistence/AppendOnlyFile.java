package com.example.miniredis.persistence;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;


public final class AppendOnlyFile implements AutoCloseable {

    private final FileOutputStream out;

    private AppendOnlyFile(FileOutputStream out) {
        this.out = out;
    }

    public static AppendOnlyFile open(Path path) throws IOException {
        return new AppendOnlyFile(new FileOutputStream(path.toFile(), true));
    }

    public synchronized void append(long atMillis, List<String> command) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        writeAscii(record, "#TS:" + atMillis + "\r\n");
        writeAscii(record, "*" + command.size() + "\r\n");
        for (String word : command) {
            byte[] bytes = word.getBytes(StandardCharsets.UTF_8);
            writeAscii(record, "$" + bytes.length + "\r\n");
            record.write(bytes, 0, bytes.length);
            writeAscii(record, "\r\n");
        }
        out.write(record.toByteArray());   // one call: the whole record or (after a crash) a cut-off record
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }

    private static void writeAscii(ByteArrayOutputStream target, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        target.write(bytes, 0, bytes.length);
    }
}