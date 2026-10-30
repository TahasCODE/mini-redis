package com.example.miniredis.server;

import com.example.miniredis.command.CommandRouter;
import com.example.miniredis.persistence.AofLoader;
import com.example.miniredis.persistence.AppendOnlyFile;
import com.example.miniredis.store.ExpirySweeper;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RedisServer {

    private final int port;
    private final Store store = new Store();
    private final CommandRouter router = new CommandRouter(store);
    private final ExpirySweeper sweeper = new ExpirySweeper(store, 100);   // runs every 100 ms
    private final ExecutorService workers = Executors.newCachedThreadPool();

    private volatile boolean running = true;
    private ServerSocket serverSocket;
    private AppendOnlyFile appendOnlyFile;

    public RedisServer(int port, Path aofPath, AppendOnlyFile.FsyncPolicy policy, long autoRewriteMinBytes)
            throws IOException {
        this.port = port;
        if (aofPath != null) {
            long started = System.nanoTime();
            long replayed = AofLoader.load(aofPath, store, router);
            System.out.println("append-only file " + aofPath.toAbsolutePath() + ": replayed " + replayed
                    + " commands in " + (System.nanoTime() - started) / 1_000_000 + " ms, "
                    + store.size() + " keys");
            appendOnlyFile = AppendOnlyFile.open(aofPath, policy, autoRewriteMinBytes);
            System.out.println("append-only file fsync policy: " + policy.name().toLowerCase()
                    + ", automatic rewrite: " + (autoRewriteMinBytes > 0 ? "from " + autoRewriteMinBytes + " bytes" : "off"));            router.enableAppendOnly(appendOnlyFile);
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        sweeper.start();
        System.out.println("MiniRedis listening on port " + port);
        while (running) {
            try {
                Socket client = serverSocket.accept();
                workers.execute(new ClientHandler(client, router));
            } catch (SocketException e) {
                if (running) {
                    throw e; // a real error; if we are stopping, this is expected
                }
            }
        }
    }

    public void stop() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        sweeper.close();
        workers.shutdownNow();
        if (appendOnlyFile != null) {
            appendOnlyFile.close();
        }
    }
}