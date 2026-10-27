package com.example.miniredis.server;

import com.example.miniredis.command.CommandRouter;
import com.example.miniredis.protocol.ProtocolException;
import com.example.miniredis.protocol.RespParser;
import com.example.miniredis.protocol.RespWriter;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

public final class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandRouter router;

    public ClientHandler(Socket socket, CommandRouter router) {
        this.socket = socket;
        this.router = router;
    }

    @Override
    public void run() {
        String client = String.valueOf(socket.getRemoteSocketAddress());
        System.out.println("client connected: " + client);
        try (Socket s = socket) {
            s.setTcpNoDelay(true);
            RespParser parser = new RespParser(s.getInputStream());
            RespWriter writer = new RespWriter(s.getOutputStream());

            while (true) {
                List<String> command;
                try {
                    command = parser.readCommand();
                } catch (ProtocolException e) {
                    writer.error("ERR Protocol error: " + e.getMessage());
                    writer.flush();
                    break;
                }
                if (command == null) {
                    break; // client closed the connection
                }
                if (command.isEmpty()) {
                    continue; // empty line, ignore
                }
                boolean keepOpen = router.execute(command, writer);
                writer.flush();
                if (!keepOpen) {
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("connection error (" + client + "): " + e.getMessage());
        }
        System.out.println("client disconnected: " + client);
    }
}