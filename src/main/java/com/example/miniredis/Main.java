package com.example.miniredis;

import com.example.miniredis.server.RedisServer;

import java.io.IOException;
import java.nio.file.Path;

public final class Main {

    private static final int DEFAULT_PORT = 6380; // real Redis uses 6379

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        // second argument: where the append-only file lives; "none" switches it off
        String aofName = args.length > 1 ? args[1] : "appendonly.aof";
        Path aofPath = aofName.equals("none") ? null : Path.of(aofName);
        RedisServer server;
        try {
            server = new RedisServer(port, aofPath);
        } catch (IOException e) {
            System.err.println("MiniRedis cannot start: " + e.getMessage());
            System.exit(1);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
                System.out.println("MiniRedis stopped");
            } catch (IOException e) {
                System.out.println("error while stopping: " + e.getMessage());
            }
        }));

        server.start();
    }
}