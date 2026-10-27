package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class CommandRouter {

    private final Store store;
    private final Map<String, CommandHandler> handlers = new HashMap<>();

    public CommandRouter(Store store) {
        this.store = store;
        handlers.put("PING", this::ping);
        handlers.put("ECHO", this::echo);
        handlers.put("COMMAND", this::command);
        handlers.put("SET", this::set);
        handlers.put("GET", this::get);
        handlers.put("DEL", this::del);
        handlers.put("EXISTS", this::exists);
        handlers.put("DBSIZE", this::dbsize);
        handlers.put("FLUSHALL", this::flushall);
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
        handler.handle(args, out);
        return true;
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


    private void set(List<String> args, RespWriter out) throws IOException {
        if (args.size() < 3) {
            wrongArgs(args, out);
            return;
        }
        if (args.size() > 3) {
            out.error("ERR syntax error");
            return;
        }
        store.set(args.get(1), args.get(2));
        out.simpleString("OK");
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

    private void wrongArgs(List<String> args, RespWriter out) throws IOException {
        out.error("ERR wrong number of arguments for '" + args.get(0).toLowerCase(Locale.ROOT) + "' command");
    }
}