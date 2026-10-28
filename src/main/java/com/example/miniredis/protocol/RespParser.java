package com.example.miniredis.protocol;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public final class RespParser {

    private static final int MAX_BULK_LENGTH = 64 * 1024 * 1024;
    private static final int MAX_ARRAY_LENGTH = 1_000_000;

    private final InputStream in;

    public RespParser(InputStream rawInput) {
        this.in = new BufferedInputStream(rawInput);
    }


    public List<String> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first == '*') {
            return readArray();
        }
        return readInline((char) first);
    }

    private List<String> readArray() throws IOException {
        String countLine = readLine();
        if (countLine == null) {
            return null;
        }
        int count = parseNumber(countLine, MAX_ARRAY_LENGTH, "invalid multibulk length");
        List<String> words = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int type = in.read();
            if (type == -1) {
                return null;
            }
            if (type != '$') {
                throw new ProtocolException("expected '$', got '" + (char) type + "'");
            }
            String lengthLine = readLine();
            if (lengthLine == null) {
                return null;
            }
            int length = parseNumber(lengthLine, MAX_BULK_LENGTH, "invalid bulk length");
            byte[] data = in.readNBytes(length);
            if (data.length != length) {
                return null;
            }
            expectCrLf();
            words.add(new String(data, StandardCharsets.UTF_8));
        }
        return words;
    }

    private List<String> readInline(char firstChar) throws IOException {
        String rest = readLine();
        if (rest == null) {
            return null;
        }
        String line = (firstChar + rest).trim();
        List<String> words = new ArrayList<>();
        if (!line.isEmpty()) {
            for (String word : line.split("\\s+")) {
                words.add(word);
            }
        }
        return words;
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next != '\n') {
                    throw new ProtocolException("expected LF after CR");
                }
                return sb.toString();
            }
            if (b == '\n') {
                return sb.toString();
            }
            sb.append((char) b);
        }
        return null;
    }

    private void expectCrLf() throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new ProtocolException("expected CRLF after bulk string");
        }
    }

    private int parseNumber(String text, int max, String errorMessage) throws ProtocolException {
        try {
            int n = Integer.parseInt(text);
            if (n < 0 || n > max) {
                throw new ProtocolException(errorMessage);
            }
            return n;
        } catch (NumberFormatException e) {
            throw new ProtocolException(errorMessage);
        }
    }
}