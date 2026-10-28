package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;
import com.example.miniredis.store.StoreException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class CounterCommands {

    private final Store store;

    CounterCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("INCR", this::incr);
        handlers.put("DECR", this::decr);
        handlers.put("INCRBY", this::incrby);
        handlers.put("DECRBY", this::decrby);
    }

    // INCR key
    private void incr(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.increment(args.get(1), 1));
    }

    // DECR key
    private void decr(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.increment(args.get(1), -1));
    }

    // INCRBY key amount
    private void incrby(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long delta = Store.parseInteger(args.get(2));
        out.integer(store.increment(args.get(1), delta));
    }

    // DECRBY key amount
    private void decrby(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long delta = Store.parseInteger(args.get(2));
        if (delta == Long.MIN_VALUE) {
            throw new StoreException("decrement would overflow");
        }
        out.integer(store.increment(args.get(1), -delta));
    }
}