package com.example.miniredis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public final class CommandDecoder {

    private static final int MAX_BULK_LENGTH = 64 * 1024 * 1024;
    private static final int MAX_ARRAY_LENGTH = 1_000_000;
    private static final int MAX_LINE = 64 * 1024;

    public record Decoded(List<String> words, int length) { }

    private CommandDecoder() { }


    public static Decoded decode(byte[] buf, int from, int to) throws ProtocolException {
        if (from >= to) {
            return null;
        }
        return buf[from] == '*' ? decodeArray(buf, from, to) : decodeInline(buf, from, to);
    }

    private static Decoded decodeArray(byte[] buf, int from, int to) throws ProtocolException {
        int pos = from + 1;
        int lineEnd = lineEnd(buf, pos, to);
        if (lineEnd < 0) {
            return null;
        }
        int count = number(buf, pos, lineEnd, MAX_ARRAY_LENGTH, "invalid multibulk length");
        pos = afterLine(buf, lineEnd);

        int[] starts = new int[count];
        int[] lengths = new int[count];
        for (int i = 0; i < count; i++) {
            if (pos >= to) {
                return null;
            }
            if (buf[pos] != '$') {
                throw new ProtocolException("expected '$', got '" + (char) (buf[pos] & 0xff) + "'");
            }
            lineEnd = lineEnd(buf, pos + 1, to);
            if (lineEnd < 0) {
                return null;
            }
            int length = number(buf, pos + 1, lineEnd, MAX_BULK_LENGTH, "invalid bulk length");
            pos = afterLine(buf, lineEnd);
            if (to - pos < length + 2) {
                return null;                       // the value (and its CRLF) has not fully arrived
            }
            if (buf[pos + length] != '\r' || buf[pos + length + 1] != '\n') {
                throw new ProtocolException("expected CRLF after bulk string");
            }
            starts[i] = pos;
            lengths[i] = length;
            pos += length + 2;
        }
        List<String> words = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            words.add(new String(buf, starts[i], lengths[i], StandardCharsets.UTF_8));
        }
        return new Decoded(words, pos - from);
    }

    private static Decoded decodeInline(byte[] buf, int from, int to) throws ProtocolException {
        int lineEnd = lineEnd(buf, from, to);
        if (lineEnd < 0) {
            return null;
        }
        String line = new String(buf, from, lineEnd - from, StandardCharsets.UTF_8).trim();
        List<String> words = new ArrayList<>();
        if (!line.isEmpty()) {
            for (String word : line.split("\\s+")) {
                words.add(word);
            }
        }
        return new Decoded(words, afterLine(buf, lineEnd) - from);
    }


    private static int lineEnd(byte[] buf, int from, int to) throws ProtocolException {
        for (int i = from; i < to; i++) {
            if (buf[i] == '\n') {
                return i;
            }
            if (buf[i] == '\r') {
                if (i + 1 >= to) {
                    return -1;                     // the LF may still be on its way
                }
                if (buf[i + 1] != '\n') {
                    throw new ProtocolException("expected LF after CR");
                }
                return i;
            }
            if (i - from > MAX_LINE) {
                throw new ProtocolException("too big inline request");
            }
        }
        return -1;
    }

    private static int afterLine(byte[] buf, int lineEnd) {
        return buf[lineEnd] == '\r' ? lineEnd + 2 : lineEnd + 1;
    }

    private static int number(byte[] buf, int from, int to, int max, String error) throws ProtocolException {
        try {
            int n = Integer.parseInt(new String(buf, from, to - from, StandardCharsets.US_ASCII));
            if (n < 0 || n > max) {
                throw new ProtocolException(error);
            }
            return n;
        } catch (NumberFormatException e) {
            throw new ProtocolException(error);
        }
    }
}