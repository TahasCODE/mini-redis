package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class ExpiryCommands {

    private final Store store;

    ExpiryCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("EXPIRE", this::expire);
        handlers.put("PEXPIRE", this::pexpire);
        handlers.put("TTL", this::ttl);
        handlers.put("PTTL", this::pttl);
        handlers.put("PERSIST", this::persist);
    }

    private void expire(List<String> args, RespWriter out) throws IOException {
        expireWith(args, out, 1000, "expire");
    }

    // PEXPIRE key milliseconds
    private void pexpire(List<String> args, RespWriter out) throws IOException {
        expireWith(args, out, 1, "pexpire");
    }

    private void expireWith(List<String> args, RespWriter out, long millisPerUnit, String name) throws IOException {
        if (args.size() != 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        String key = args.get(1);
        long amount = Store.parseInteger(args.get(2));
        if (amount <= 0) {
            out.integer(store.delete(key) ? 1 : 0);
            return;
        }
        long expiresAt = CommandSupport.toExpiresAt(amount, millisPerUnit, name);
        out.integer(store.setExpiry(key, expiresAt) ? 1 : 0);
    }

    private void ttl(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long ms = store.ttlMillis(args.get(1));
        out.integer(ms < 0 ? ms : (ms + 500) / 1000);
    }

    private void pttl(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.ttlMillis(args.get(1)));
    }

    private void persist(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        out.integer(store.persist(args.get(1)) ? 1 : 0);
    }
}