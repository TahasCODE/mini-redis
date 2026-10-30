package com.example.miniredis;

import com.example.miniredis.persistence.AppendOnlyFile;
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
        // third argument: fsync policy (always, everysec or no); default everysec, like real Redis
        AppendOnlyFile.FsyncPolicy policy = AppendOnlyFile.FsyncPolicy.EVERYSEC;
        // fourth argument: rewrite the file by itself from this many megabytes (0 = never); default 8
        long autoRewriteMinBytes = 8L * 1024 * 1024;
        try {
            if (args.length > 2) {
                policy = AppendOnlyFile.FsyncPolicy.parse(args[2]);
            }
            if (args.length > 3) {
                autoRewriteMinBytes = (long) (Double.parseDouble(args[3]) * 1024 * 1024);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("MiniRedis cannot start: " + e.getMessage());
            System.exit(1);
            return;
        }
        RedisServer server;
        try {
            server = new RedisServer(port, aofPath, policy, autoRewriteMinBytes);
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