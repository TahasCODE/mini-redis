package com.example.miniredis.protocol;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class RespWriter {

    private static final byte[] CRLF = {'\r', '\n'};

    private final OutputStream out;

    public RespWriter(OutputStream rawOutput) {
        this.out = new BufferedOutputStream(rawOutput);
    }

    /** +OK\r\n */
    public void simpleString(String text) throws IOException {
        writeLine('+' + text);
    }

    /** -ERR something went wrong\r\n */
    public void error(String message) throws IOException {
        writeLine('-' + message);
    }

    /** :42\r\n */
    public void integer(long value) throws IOException {
        writeLine(':' + Long.toString(value));
    }

    /** $5\r\nhello\r\n  (a null value is written as $-1\r\n) */
    public void bulkString(String text) throws IOException {
        if (text == null) {
            writeLine("$-1");
            return;
        }
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        writeLine("$" + bytes.length);
        out.write(bytes);
        out.write(CRLF);
    }

    /** *3\r\n  - the caller then writes the 3 elements. */
    public void arrayHeader(int size) throws IOException {
        writeLine("*" + size);
    }

    public void flush() throws IOException {
        out.flush();
    }

    private void writeLine(String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }
}