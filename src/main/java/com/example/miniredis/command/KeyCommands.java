package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;
import com.example.miniredis.store.StoreException;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class KeyCommands {

    private static final int DEFAULT_SCAN_COUNT = 10;

    private final Store store;

    KeyCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("KEYS", this::keys);
        handlers.put("SCAN", this::scan);
    }

    private void keys(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        List<String> found = store.keys(args.get(1));
        out.arrayHeader(found.size());
        for (String key : found) {
            out.bulkString(key);
        }
    }

    private void scan(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long cursor = parseCursor(args.get(1));
        String pattern = null;
        int count = DEFAULT_SCAN_COUNT;
        for (int i = 2; i < args.size(); i += 2) {
            String option = args.get(i).toUpperCase(Locale.ROOT);
            if (i + 1 >= args.size()) {
                CommandSupport.syntaxError(out);
                return;
            }
            String value = args.get(i + 1);
            if (option.equals("MATCH")) {
                pattern = value;
            } else if (option.equals("COUNT")) {
                long parsed = Store.parseInteger(value);
                if (parsed < 1 || parsed > Integer.MAX_VALUE) {
                    CommandSupport.syntaxError(out);
                    return;
                }
                count = (int) parsed;
            } else {
                CommandSupport.syntaxError(out);
                return;
            }
        }
        Store.ScanResult result = store.scan(cursor, count, pattern);
        out.arrayHeader(2);
        out.bulkString(Long.toString(result.nextCursor()));
        out.arrayHeader(result.keys().size());
        for (String key : result.keys()) {
            out.bulkString(key);
        }
    }

    private static long parseCursor(String text) {
        try {
            long cursor = Long.parseUnsignedLong(text);
            return cursor < 0 ? Long.MAX_VALUE : cursor;
        } catch (NumberFormatException e) {
            throw new StoreException("invalid cursor");
        }
    }
}