package com.example.miniredis;

import com.example.miniredis.server.RedisServer;

import java.io.IOException;

public final class Main {

    private static final int DEFAULT_PORT = 6380; // real Redis uses 6379

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        RedisServer server = new RedisServer(port);

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