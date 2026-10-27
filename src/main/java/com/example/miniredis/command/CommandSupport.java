package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.StoreException;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

final class CommandSupport {

    private CommandSupport() {
    }

    static void wrongArgs(List<String> args, RespWriter out) throws IOException {
        out.error("ERR wrong number of arguments for '" + args.get(0).toLowerCase(Locale.ROOT) + "' command");
    }

    static void syntaxError(RespWriter out) throws IOException {
        out.error("ERR syntax error");
    }


    static long toExpiresAt(long amount, long millisPerUnit, String commandName) {
        if (amount <= 0) {
            throw new StoreException("invalid expire time in '" + commandName + "' command");
        }
        try {
            return Math.addExact(System.currentTimeMillis(), Math.multiplyExact(amount, millisPerUnit));
        } catch (ArithmeticException e) {
            throw new StoreException("invalid expire time in '" + commandName + "' command");
        }
    }
}