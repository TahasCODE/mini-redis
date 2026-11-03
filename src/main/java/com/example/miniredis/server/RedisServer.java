package com.example.miniredis.server;

import com.example.miniredis.command.CommandRouter;
import com.example.miniredis.persistence.AofLoader;
import com.example.miniredis.persistence.AppendOnlyFile;
import com.example.miniredis.protocol.CommandDecoder;
import com.example.miniredis.protocol.ProtocolException;
import com.example.miniredis.store.ExpirySweeper;
import com.example.miniredis.store.Store;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;


public final class RedisServer {

    private static final int OUTPUT_HIGH_WATER = 1024 * 1024;
    private static final int MAX_INPUT = 80 * 1024 * 1024;

    private final int port;
    private final Store store = new Store();
    private final CommandRouter router = new CommandRouter(store);
    private final ExpirySweeper sweeper = new ExpirySweeper(store, 100);   // runs every 100 ms
    private final ByteBuffer scratch = ByteBuffer.allocate(64 * 1024);     // one read buffer for everybody

    private volatile boolean running = true;
    private Selector selector;
    private ServerSocketChannel serverChannel;
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
                    + ", automatic rewrite: " + (autoRewriteMinBytes > 0 ? "from " + autoRewriteMinBytes + " bytes" : "off"));
            router.enableAppendOnly(appendOnlyFile);
        }
    }

    public void start() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        sweeper.start();
        System.out.println("MiniRedis listening on port " + port + " (event loop, 1 thread)");

        while (running) {
            selector.select();                                   // sleeps until a connection needs attention
            Iterator<SelectionKey> ready = selector.selectedKeys().iterator();
            while (ready.hasNext()) {
                SelectionKey key = ready.next();
                ready.remove();
                if (!key.isValid()) {
                    continue;
                }
                try {
                    if (key.isAcceptable()) {
                        accept();
                    } else {
                        Connection c = (Connection) key.attachment();
                        if (key.isWritable()) {
                            onWritable(key, c);
                        } else if (key.isReadable()) {
                            onReadable(key, c);
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    // a problem with ONE client must never stop the loop for all the others
                    Object attachment = key.attachment();
                    if (attachment instanceof Connection c) {
                        System.out.println("connection error (" + c.name + "): " + e);
                        close(key, c);
                    } else if (running) {
                        throw e;
                    }
                }
            }
        }
    }

    private void accept() throws IOException {
        SocketChannel channel = serverChannel.accept();
        if (channel == null) {
            return;
        }
        channel.configureBlocking(false);
        channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
        Connection c = new Connection(channel, String.valueOf(channel.getRemoteAddress()));
        channel.register(selector, SelectionKey.OP_READ, c);
        System.out.println("client connected: " + c.name);
    }

    private void onReadable(SelectionKey key, Connection c) throws IOException {
        if (!c.readFromChannel(scratch)) {
            close(key, c);                                       // the client closed the connection
            return;
        }
        if (c.inputSize() > MAX_INPUT) {
            c.writer.error("ERR Protocol error: query buffer too big");
            c.closeAfterWrite = true;
        }
        processInput(key, c);
    }

    private void onWritable(SelectionKey key, Connection c) throws IOException {
        if (!c.flush()) {
            return;                                              // still not everything: stay in OP_WRITE
        }
        if (c.closeAfterWrite) {
            close(key, c);
            return;
        }
        key.interestOps(SelectionKey.OP_READ);                   // the slow client caught up: read again
        processInput(key, c);                                    // commands may have been waiting in the buffer
    }

    /** Runs every complete command in the client's input buffer, then sends the replies. */
    private void processInput(SelectionKey key, Connection c) throws IOException {
        while (true) {
            boolean stoppedForOutput = false;
            while (!c.closeAfterWrite) {
                if (c.pendingBytes() >= OUTPUT_HIGH_WATER) {
                    stoppedForOutput = true;                     // too many unsent replies: pause this client
                    break;
                }
                CommandDecoder.Decoded d;
                try {
                    d = CommandDecoder.decode(c.inputBytes(), c.inputStart(), c.inputEnd());
                } catch (ProtocolException e) {
                    c.writer.error("ERR Protocol error: " + e.getMessage());
                    c.closeAfterWrite = true;
                    break;
                }
                if (d == null) {
                    break;                                       // the rest of the command has not arrived yet
                }
                c.consume(d.length());
                List<String> command = d.words();
                if (command.isEmpty()) {
                    continue;                                    // empty line, ignore
                }
                if (!router.execute(command, c.writer, c.session)) {
                    c.closeAfterWrite = true;                    // QUIT
                }
            }
            if (!c.flush()) {
                key.interestOps(SelectionKey.OP_WRITE);          // slow client: read nothing more until it caught up
                return;
            }
            if (c.closeAfterWrite) {
                close(key, c);
                return;
            }
            key.interestOps(SelectionKey.OP_READ);
            if (!stoppedForOutput) {
                return;                                          // everything in the buffer was handled
            }
        }
    }

    private void close(SelectionKey key, Connection c) {
        key.cancel();
        try {
            c.channel.close();
        } catch (IOException ignored) {
            // nothing more to do
        }
        System.out.println("client disconnected: " + c.name);
    }

    public void stop() throws IOException {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
        if (serverChannel != null) {
            serverChannel.close();
        }
        sweeper.close();
        if (appendOnlyFile != null) {
            appendOnlyFile.close();
        }
    }
}