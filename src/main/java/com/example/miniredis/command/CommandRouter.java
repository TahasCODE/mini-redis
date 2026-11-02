package com.example.miniredis.command;

import com.example.miniredis.persistence.AppendOnlyFile;
import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;
import com.example.miniredis.store.StoreException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class CommandRouter {

    private final Store store;
    private final Map<String, CommandHandler> handlers = new HashMap<>();

    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "SETEX", "PSETEX", "DEL", "FLUSHALL", "INCR", "DECR", "INCRBY", "DECRBY",
            "EXPIRE", "PEXPIRE", "PERSIST", "LPUSH", "RPUSH", "LPOP", "RPOP", "LTRIM",
            "HSET", "HDEL", "HINCRBY");


    private static final Map<String, Integer> ARITY = Map.ofEntries(
            Map.entry("PING", -1), Map.entry("ECHO", 2), Map.entry("COMMAND", -1), Map.entry("GET", 2),
            Map.entry("DEL", -2), Map.entry("EXISTS", -2), Map.entry("DBSIZE", 1), Map.entry("FLUSHALL", -1),
            Map.entry("TYPE", 2), Map.entry("BGREWRITEAOF", 1),
            Map.entry("SET", -3), Map.entry("SETEX", 4), Map.entry("PSETEX", 4),
            Map.entry("INCR", 2), Map.entry("DECR", 2), Map.entry("INCRBY", 3), Map.entry("DECRBY", 3),
            Map.entry("EXPIRE", 3), Map.entry("PEXPIRE", 3), Map.entry("TTL", 2), Map.entry("PTTL", 2),
            Map.entry("PERSIST", 2),
            Map.entry("LPUSH", -3), Map.entry("RPUSH", -3), Map.entry("LPOP", -2), Map.entry("RPOP", -2),
            Map.entry("LLEN", 2), Map.entry("LRANGE", 4), Map.entry("LINDEX", 3), Map.entry("LTRIM", 4),
            Map.entry("HSET", -4), Map.entry("HGET", 3), Map.entry("HDEL", -3), Map.entry("HGETALL", 2),
            Map.entry("HKEYS", 2), Map.entry("HVALS", 2), Map.entry("HLEN", 2), Map.entry("HEXISTS", 3),
            Map.entry("HINCRBY", 4), Map.entry("KEYS", 2), Map.entry("SCAN", -2));


    private final ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock();
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
        return execute(args, out, null);
    }


    public boolean execute(List<String> args, RespWriter out, ClientSession session) throws IOException {
        String name = args.get(0).toUpperCase(Locale.ROOT);

        if (name.equals("QUIT")) {
            out.simpleString("OK");
            return false;
        }

        if (session != null) {
            switch (name) {
                case "MULTI" -> {
                    multi(args, out, session);
                    return true;
                }
                case "DISCARD" -> {
                    discard(args, out, session);
                    return true;
                }
                case "EXEC" -> {
                    exec(args, out, session);
                    return true;
                }
                default -> {
                    // inside MULTI nothing is run: the command is checked and put in the queue
                    if (session.inMulti) {
                        queue(name, args, out, session);
                        return true;
                    }
                }
            }
        }

        CommandHandler handler = handlers.get(name);
        if (handler == null) {
            out.error("ERR unknown command '" + args.get(0) + "'");
            return true;
        }
        transactionLock.readLock().lock();      // shared: many commands at once, but never during an EXEC
        try {
            AppendOnlyFile log = appendOnlyFile;
            if (log != null && WRITE_COMMANDS.contains(name)) {
                runAndLog(handler, args, out, log);
            } else {
                handler.handle(args, out);
            }
        } catch (StoreException e) {
            out.error(e.reply());
        } finally {
            transactionLock.readLock().unlock();
        }
        return true;
    }

    private void multi(List<String> args, RespWriter out, ClientSession session) throws IOException {
        if (args.size() != 1) {
            wrongArgs(args, out);
        } else if (session.inMulti) {
            out.error("ERR MULTI calls can not be nested");     // the transaction goes on, it is not broken by this
        } else {
            session.startTransaction();
            out.simpleString("OK");
        }
    }

    private void discard(List<String> args, RespWriter out, ClientSession session) throws IOException {
        if (args.size() != 1) {
            wrongArgs(args, out);
        } else if (!session.inMulti) {
            out.error("ERR DISCARD without MULTI");
        } else {
            session.reset();
            out.simpleString("OK");
        }
    }

    private void queue(String name, List<String> args, RespWriter out, ClientSession session) throws IOException {
        if (!handlers.containsKey(name)) {
            out.error("ERR unknown command '" + args.get(0) + "'");
            session.dirty = true;
            return;
        }
        Integer arity = ARITY.get(name);
        if (arity != null && !(arity >= 0 ? args.size() == arity : args.size() >= -arity)) {
            wrongArgs(args, out);
            session.dirty = true;
            return;
        }
        session.queued.add(List.copyOf(args));
        out.simpleString("QUEUED");
    }

    private void exec(List<String> args, RespWriter out, ClientSession session) throws IOException {
        if (args.size() != 1) {
            wrongArgs(args, out);
            return;
        }
        if (!session.inMulti) {
            out.error("ERR EXEC without MULTI");
            return;
        }
        List<List<String>> queued = new ArrayList<>(session.queued);
        boolean broken = session.dirty;
        session.reset();                                        // the transaction is over, whatever happens next
        if (broken) {
            out.error("EXECABORT Transaction discarded because of previous errors.");
            return;
        }

        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        RespWriter replies = new RespWriter(collected);
        transactionLock.writeLock().lock();                     // exclusive: nobody else runs a command meanwhile
        try {
            AppendOnlyFile log = appendOnlyFile;
            if (log == null) {
                runQueued(queued, replies, null);
            } else {
                synchronized (log) {
                    runQueued(queued, replies, log);
                }
            }
        } finally {
            transactionLock.writeLock().unlock();
        }
        replies.flush();
        out.writeRaw(collected.toByteArray());
    }

    private void runQueued(List<List<String>> queued, RespWriter replies, AppendOnlyFile log) throws IOException {
        long startedAt = store.now();
        List<List<String>> toLog = new ArrayList<>();
        replies.arrayHeader(queued.size());
        for (List<String> command : queued) {
            String name = command.get(0).toUpperCase(Locale.ROOT);
            replies.clearErrorFlag();
            try {
                handlers.get(name).handle(command, replies);
            } catch (StoreException e) {
                replies.error(e.reply());                       // WRONGTYPE and friends: this reply is an error
            }
            if (log != null && !replies.errorWritten() && WRITE_COMMANDS.contains(name)) {
                toLog.add(command);                             // only what worked is written to the file
            }
        }
        replies.clearErrorFlag();
        if (!toLog.isEmpty()) {
            log.appendTransaction(startedAt, toLog);            // one block: MULTI ... EXEC
            if (log.shouldRewrite()) {
                rewriteNow(log);
            }
        }
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

    // FLUSHALL -> delete every key
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