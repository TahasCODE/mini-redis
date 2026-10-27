package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** SET (with NX, XX, EX, PX), SETEX and PSETEX. */
final class StringCommands {

    private final Store store;

    StringCommands(Store store) {
        this.store = store;
    }

    void register(Map<String, CommandHandler> handlers) {
        handlers.put("SET", this::set);
        handlers.put("SETEX", this::setex);
        handlers.put("PSETEX", this::psetex);
    }

    // SET key value [NX | XX] [EX seconds | PX milliseconds]
    private void set(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 3) {
            CommandSupport.wrongArgs(args, out);
            return;
        }

        Store.SetCondition condition = Store.SetCondition.ALWAYS;
        long expiresAt = Store.NO_EXPIRY;
        String expiryUnit = null;   // "EX" or "PX" once one of them has been seen

        int i = 3;
        while (i < args.size()) {
            String option = args.get(i).toUpperCase(Locale.ROOT);
            switch (option) {
                case "NX", "XX" -> {
                    Store.SetCondition wanted = option.equals("NX")
                            ? Store.SetCondition.ONLY_IF_ABSENT
                            : Store.SetCondition.ONLY_IF_PRESENT;
                    if (condition != Store.SetCondition.ALWAYS && condition != wanted) {
                        CommandSupport.syntaxError(out);   // NX and XX together make no sense
                        return;
                    }
                    condition = wanted;
                    i++;
                }
                case "EX", "PX" -> {
                    if (i + 1 >= args.size() || (expiryUnit != null && !expiryUnit.equals(option))) {
                        CommandSupport.syntaxError(out);   // no number after it, or EX mixed with PX
                        return;
                    }
                    long amount = Store.parseInteger(args.get(i + 1));
                    long millisPerUnit = option.equals("EX") ? 1000 : 1;
                    expiresAt = CommandSupport.toExpiresAt(amount, millisPerUnit, "set");
                    expiryUnit = option;
                    i += 2;
                }
                default -> {
                    CommandSupport.syntaxError(out);
                    return;
                }
            }
        }

        boolean written = store.set(args.get(1), args.get(2), expiresAt, condition);
        if (written) {
            out.simpleString("OK");
        } else {
            out.bulkString(null);   // NX or XX blocked the write: reply null
        }
    }

    // SETEX key seconds value
    private void setex(List<String> args, RespWriter out) throws IOException {
        setWithTtl(args, out, 1000, "setex");
    }

    // PSETEX key milliseconds value
    private void psetex(List<String> args, RespWriter out) throws IOException {
        setWithTtl(args, out, 1, "psetex");
    }

    private void setWithTtl(List<String> args, RespWriter out, long millisPerUnit, String name) throws IOException {
        if (args.size() != 4) {
            CommandSupport.wrongArgs(args, out);
            return;
        }
        long amount = Store.parseInteger(args.get(2));
        long expiresAt = CommandSupport.toExpiresAt(amount, millisPerUnit, name);
        store.set(args.get(1), args.get(3), expiresAt, Store.SetCondition.ALWAYS);
        out.simpleString("OK");
    }
}