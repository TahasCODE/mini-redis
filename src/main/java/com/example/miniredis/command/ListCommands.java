package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;
import com.example.miniredis.store.StoreException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class ListCommands {

    private final Store store;

    ListCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("LPUSH", (args, out) -> push(args, out, true));
        handlers.put("RPUSH", (args, out) -> push(args, out, false));
        handlers.put("LPOP", (args, out) -> pop(args, out, true));
        handlers.put("RPOP", (args, out) -> pop(args, out, false));
        handlers.put("LLEN", this::llen);
        handlers.put("LRANGE", this::lrange);
        handlers.put("LINDEX", this::lindex);
    }

    private void push(List<String> args, RespWriter out, boolean atFront) throws IOException {
        if (args.size() < 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.push(args.get(1), args.subList(2, args.size()), atFront));
    }

    private void pop(List<String> args, RespWriter out, boolean fromFront) throws IOException {
        if (args.size() < 2 || args.size() > 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        String key = args.get(1);
        if (args.size() == 2) {
            List<String> removed = store.pop(key, 1, fromFront);
            out.bulkString(removed == null ? null : removed.get(0));
            return;
        }
        long count = parseCount(args.get(2));
        List<String> removed = store.pop(key, count, fromFront);
        if (removed == null) {
            out.arrayHeader(-1);   // a "null array": the key does not exist
            return;
        }
        writeList(out, removed);
    }

    private static long parseCount(String text) {
        long count;
        try {
            count = Store.parseInteger(text);
        } catch (StoreException e) {
            count = -1;
        }
        if (count < 0) {
            throw new StoreException("value is out of range, must be positive");
        }
        return count;
    }

    private void llen(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.listLength(args.get(1)));
    }

    private void lrange(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 4) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long start = Store.parseInteger(args.get(2));
        long stop = Store.parseInteger(args.get(3));
        writeList(out, store.range(args.get(1), start, stop));
    }

    private void lindex(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long index = Store.parseInteger(args.get(2));
        out.bulkString(store.index(args.get(1), index));
    }

    private static void writeList(RespWriter out, List<String> items) throws IOException {
        out.arrayHeader(items.size());
        for (String item : items) {
            out.bulkString(item);
        }
    }
}