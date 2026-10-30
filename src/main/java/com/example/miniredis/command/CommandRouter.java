package com.example.miniredis.command;

import com.example.miniredis.persistence.AppendOnlyFile;
import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;
import com.example.miniredis.store.StoreException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class CommandRouter {

    private final Store store;
    private final Map<String, CommandHandler> handlers = new HashMap<>();

    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "SETEX", "PSETEX", "DEL", "FLUSHALL", "INCR", "DECR", "INCRBY", "DECRBY",
            "EXPIRE", "PEXPIRE", "PERSIST", "LPUSH", "RPUSH", "LPOP", "RPOP", "LTRIM",
            "HSET", "HDEL", "HINCRBY");

    private volatile AppendOnlyFile appendOnlyFile;

    public CommandRouter(Store store) {
        this.store = store;
        handlers.put("PING", this::ping);
        handlers.put("ECHO", this::echo);
        handlers.put("COMMAND", this::command);
        handlers.put("GET", this::get);
        handlers.put("DEL", this::del);
        handlers.put("EXISTS", this::exists);
        handlers.put("DBSIZE", this::dbsize);
        handlers.put("FLUSHALL", this::flushall);
        handlers.put("TYPE", this::type);
        handlers.put("BGREWRITEAOF", this::rewriteAof);

        new StringCommands(store).register(handlers);
        new CounterCommands(store).register(handlers);
        new ExpiryCommands(store).register(handlers);
        new ListCommands(store).register(handlers);
        new HashCommands(store).register(handlers);
        new KeyCommands(store).register(handlers);
    }

    public void enableAppendOnly(AppendOnlyFile file) {
        this.appendOnlyFile = file;
    }


    public boolean execute(List<String> args, RespWriter out) throws IOException {
        String name = args.get(0).toUpperCase(Locale.ROOT);

        if (name.equals("QUIT")) {
            out.simpleString("OK");
            return false;
        }

        CommandHandler handler = handlers.get(name);
        if (handler == null) {
            out.error("ERR unknown command '" + args.get(0) + "'");
            return true;
        }
        try {
            AppendOnlyFile log = appendOnlyFile;
            if (log != null && WRITE_COMMANDS.contains(name)) {
                runAndLog(handler, args, out, log);
            } else {
                handler.handle(args, out);
            }
        } catch (StoreException e) {
            out.error(e.reply());
        }
        return true;
    }


    private void runAndLog(CommandHandler handler, List<String> args, RespWriter out, AppendOnlyFile log)
            throws IOException {
        synchronized (log) {
            long startedAt = store.now();
            out.clearErrorFlag();
            handler.handle(args, out);           // a StoreException flies out of here and nothing is logged
            if (!out.errorWritten()) {           // wrong arguments and syntax errors are replies, not exceptions
                log.append(startedAt, args);
                if (log.shouldRewrite()) {       // the file has grown a lot: shrink it (still under the lock)
                    rewriteNow(log);
                }
            }
        }
    }

    private void rewriteNow(AppendOnlyFile log) {
        try {
            AppendOnlyFile.RewriteResult r = log.rewrite(store);
            System.out.println("append-only file rewritten: " + r.bytesBefore() + " -> " + r.bytesAfter() + " bytes, "
                    + r.keys() + " keys, " + r.commands() + " commands, " + r.millis() + " ms");
        } catch (IOException | RuntimeException e) {
            System.out.println("append-only file rewrite failed (the old file is still in use): " + e);
        }
    }

    private void rewriteAof(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 1) {
            wrongArgs(args, out);
            return;
        }
        AppendOnlyFile log = appendOnlyFile;
        if (log == null) {
            out.error("ERR the append-only file is not enabled");
            return;
        }
        synchronized (log) {
            try {
                AppendOnlyFile.RewriteResult r = log.rewrite(store);
                System.out.println("append-only file rewritten: " + r.bytesBefore() + " -> " + r.bytesAfter()
                        + " bytes, " + r.keys() + " keys, " + r.commands() + " commands, " + r.millis() + " ms");
            } catch (IOException e) {
                out.error("ERR rewrite failed, the old file is still in use: " + e.getMessage());
                return;
            }
        }
        out.simpleString("Background append only file rewriting started");
    }


    private void ping(List<String> args, RespWriter out) throws IOException {
        if (args.size() == 1) {
            out.simpleString("PONG");
        } else if (args.size() == 2) {
            out.bulkString(args.get(1));
        } else {
            wrongArgs(args, out);
        }
    }

    private void echo(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            wrongArgs(args, out);
            return;
        }
        out.bulkString(args.get(1));
    }

    private void command(List<String> args, RespWriter out) throws IOException {
        out.arrayHeader(0);
    }

    private void get(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            wrongArgs(args, out);
            return;
        }
        out.bulkString(store.get(args.get(1)));
    }

    private void del(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 2) {
            wrongArgs(args, out);
            return;
        }
        long deleted = 0;
        for (int i = 1; i < args.size(); i++) {
            if (store.delete(args.get(i))) {
                deleted++;
            }
        }
        out.integer(deleted);
    }

    private void exists(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 2) {
            wrongArgs(args, out);
            return;
        }
        long count = 0;
        for (int i = 1; i < args.size(); i++) {
            if (store.exists(args.get(i))) {
                count++;
            }
        }
        out.integer(count);
    }

    private void dbsize(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 1) {
            wrongArgs(args, out);
            return;
        }
        out.integer(store.size());
    }

    private void flushall(List<String> args, RespWriter out) throws IOException {
        store.clear();
        out.simpleString("OK");
    }

    private void type(List<String> args, RespWriter out) throws IOException {
        if (args.size() != 2) {
            wrongArgs(args, out);
            return;
        }
        out.simpleString(store.type(args.get(1)));
    }

    private void wrongArgs(List<String> args, RespWriter out) throws IOException {
        out.error("ERR wrong number of arguments for '" + args.get(0).toLowerCase(Locale.ROOT) + "' command");
    }
}