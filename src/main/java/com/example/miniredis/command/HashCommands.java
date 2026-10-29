package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class HashCommands {

    private final Store store;

    HashCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("HSET", this::hset);
        handlers.put("HGET", this::hget);
        handlers.put("HDEL", this::hdel);
        handlers.put("HGETALL", this::hgetall);
        handlers.put("HKEYS", (args, out) -> hlist(args, out, true, false));
        handlers.put("HVALS", (args, out) -> hlist(args, out, false, true));
        handlers.put("HLEN", this::hlen);
        handlers.put("HEXISTS", this::hexists);
        handlers.put("HINCRBY", this::hincrby);
    }

    private void hset(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 4 || args.size() % 2 != 0) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.hashSet(args.get(1), args.subList(2, args.size())));
    }

    private void hget(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.bulkString(store.hashGet(args.get(1), args.get(2)));
    }

    private void hdel(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.hashDelete(args.get(1), args.subList(2, args.size())));
    }

    private void hgetall(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        Map<String, String> fields = store.hashGetAll(args.get(1));
        out.arrayHeader(fields.size() * 2);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            out.bulkString(field.getKey());
            out.bulkString(field.getValue());
        }
    }

    private void hlist(List<String> args, RespWriter out, boolean names, boolean values) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        Map<String, String> fields = store.hashGetAll(args.get(1));
        out.arrayHeader(fields.size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            out.bulkString(names ? field.getKey() : field.getValue());
        }
    }

    private void hlen(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.hashLength(args.get(1)));
    }

    private void hexists(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.hashGet(args.get(1), args.get(2)) != null ? 1 : 0);
    }

    private void hincrby(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 4) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long delta = Store.parseInteger(args.get(3));
        out.integer(store.hashIncrement(args.get(1), args.get(2), delta));
    }
}