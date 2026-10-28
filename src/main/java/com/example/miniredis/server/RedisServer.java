package com.example.miniredis.server;

import com.example.miniredis.command.CommandRouter;
import com.example.miniredis.store.ExpirySweeper;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
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

    public RedisServer(int port) {
        this.port = port;
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
                    throw e;
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
    }
}