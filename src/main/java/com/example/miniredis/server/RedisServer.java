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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public final class RedisServer {

    private static final int OUTPUT_HIGH_WATER = 1024 * 1024;
    private static final int MAX_INPUT = 80 * 1024 * 1024;

    private static final int PUBSUB_OUTPUT_LIMIT = 8 * 1024 * 1024;
    private final PubSub pubsub = new PubSub();

    private final int port;
    private final Store store = new Store();
    private final CommandRouter router = new CommandRouter(store);
    private final ExpirySweeper sweeper = new ExpirySweeper(store, 100);   // runs every 100 ms
    private final ByteBuffer scratch = ByteBuffer.allocate(64 * 1024);     // one read buffer for everybody
    private List<Connection> toSend = new ArrayList<>();

    private volatile boolean running = true;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private AppendOnlyFile appendOnlyFile;


    public RedisServer(int port, Path aofPath, AppendOnlyFile.FsyncPolicy policy, long autoRewriteMinBytes)
            throws IOException {
        this.port = port;
        router.enablePublisher((args, out) -> {
            if (args.size() != 3) {
                out.error("ERR wrong number of arguments for 'publish' command");
            } else {
                out.integer(pubsub.publish(args.get(1), args.get(2), this::enqueueMessage));
            }
        });
        if (aofPath != null) {
            long started = System.nanoTime();
            long replayed = AofLoader.load(aofPath, store, router);
            System.out.println("append-only file " + aofPath.toAbsolutePath() + ": replayed " + replayed
                    + " commands in " + (System.nanoTime() - started) / 1_000_000 + " ms, "
                    + store.size() + " keys");
            appendOnlyFile = AppendOnlyFile.open(aofPath, policy, autoRewriteMinBytes);
            System.out.println("append-only file fsync policy: " + policy.name().toLowerCase()
                    + ", automatic rewrite: " + (autoRewriteMinBytes > 0 ? "from " + autoRewriteMinBytes + " bytes" : "off"));
            appendOnlyFile.setGroupCommit(true);     // Day 10: fsync once per round of commands, not once per command
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
        System.out.println("MiniRedis listening on port " + port + " (event loop, 1 thread, group commit)");

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
                            onWritable(c);
                        } else if (key.isReadable()) {
                            onReadable(c);
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    // a problem with ONE client must never stop the loop for all the others
                    Object attachment = key.attachment();
                    if (attachment instanceof Connection c) {
                        System.out.println("connection error (" + c.name + "): " + e);
                        close(c);
                    } else if (running) {
                        throw e;
                    }
                }
            }
            // Every command of this round has run. NOW the group commit: one fsync for all of them, and only then
            // the replies are sent. If the fsync fails the IOException ends the server: we must not tell clients
            // "OK" for data that is not on the disk (real Redis stops in this case too).
            finishRound();
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
        c.key = channel.register(selector, SelectionKey.OP_READ, c);
        System.out.println("client connected: " + c.name);
    }

    private void onReadable(Connection c) throws IOException {
        if (!c.readFromChannel(scratch)) {
            close(c);                                            // the client closed the connection
            return;
        }
        if (c.inputSize() > MAX_INPUT) {
            c.writer.error("ERR Protocol error: query buffer too big");
            c.closeAfterWrite = true;
        }
        processInput(c);
    }

    private void onWritable(Connection c) throws IOException {
        if (!c.flush()) {
            return;                                              // still not everything: stay in OP_WRITE
        }
        if (c.closeAfterWrite) {
            close(c);
            return;
        }
        c.key.interestOps(SelectionKey.OP_READ);                 // the slow client caught up: read again
        processInput(c);                                         // commands may have been waiting in the buffer
    }


    private void processInput(Connection c) throws IOException {
        c.pausedForOutput = false;
        while (!c.closeAfterWrite) {
            if (c.pendingBytes() >= OUTPUT_HIGH_WATER) {
                c.pausedForOutput = true;                        // too many unsent replies: pause this client
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
                break;                                           // the rest of the command has not arrived yet
            }
            c.consume(d.length());
            List<String> command = d.words();
            if (command.isEmpty()) {
                continue;                                        // empty line, ignore
            }
            if (handlePubSub(c, command)) {
                if (c.closed) return;
                continue;
            }
            if (!router.execute(command, c.writer, c.session)) {
                c.closeAfterWrite = true;                        // QUIT
            }
        }
        schedule(c);
    }

    private void schedule(Connection c) {
        if (!c.closed && !c.scheduled) {
            c.scheduled = true;
            toSend.add(c);                                       // its replies go out after the commit
        }
    }

    private void finishRound() throws IOException {
        while (!toSend.isEmpty()) {
            if (appendOnlyFile != null) {
                appendOnlyFile.commit();                         // ONE fsync for all the writes of this round
            }
            List<Connection> batch = toSend;
            toSend = new ArrayList<>();
            for (Connection c : batch) {
                c.scheduled = false;
                send(c);                                         // may schedule c again (more buffered commands)
            }
        }
    }

    private void send(Connection c) {
        SelectionKey key = c.key;
        if (!key.isValid()) {
            return;                                              // the connection was closed in this round
        }
        try {
            if (!c.flush()) {
                key.interestOps(SelectionKey.OP_WRITE);          // slow client: read nothing more until it caught up
                return;
            }
            if (c.closeAfterWrite) {
                close(c);
                return;
            }
            key.interestOps(SelectionKey.OP_READ);
            if (c.pausedForOutput) {
                processInput(c);                                 // there are buffered commands left: next pass
            }
        } catch (IOException | RuntimeException e) {
            System.out.println("connection error (" + c.name + "): " + e);
            close(c);
        }
    }

    private boolean handlePubSub(Connection c, List<String> args) throws IOException {
        String name = args.getFirst().toUpperCase(java.util.Locale.ROOT);
        boolean subscribed = !c.subscriptions.isEmpty();
        if (subscribed && !java.util.Set.of("SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT").contains(name)) {
            c.writer.error("ERR Can't execute '" + name.toLowerCase(java.util.Locale.ROOT)
                    + "': only SUBSCRIBE, UNSUBSCRIBE, PING and QUIT are allowed in this context");
        } else if ((name.equals("SUBSCRIBE") || name.equals("UNSUBSCRIBE")) && !c.session.inTransaction()) {
            if (name.equals("SUBSCRIBE")) pubsub.subscribe(c, args);
            else pubsub.unsubscribe(c, args);
        } else if (subscribed && name.equals("PING")) {
            if (args.size() > 2) c.writer.error("ERR wrong number of arguments for 'ping' command");
            else {
                c.writer.arrayHeader(2);
                c.writer.bulkString("pong");
                c.writer.bulkString(args.size() == 2 ? args.get(1) : "");
            }
        } else {
            return false;
        }
        c.writer.flush();
        if (c.pendingBytes() > PUBSUB_OUTPUT_LIMIT) close(c);
        else schedule(c);
        return true;
    }

    private boolean enqueueMessage(Connection c, String channel, String message) {
        if (c.closed || c.closeAfterWrite || !c.key.isValid()) return false;
        try {
            c.writer.flush();
            // Compute RESP size in bytes before allocating another pending message.
            long a = channel.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            long b = message.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            long size = 4 + 13 + 5 + Long.toString(a).length() + a
                    + 5 + Long.toString(b).length() + b;
            if (c.pendingBytes() + size > PUBSUB_OUTPUT_LIMIT) {
                close(c); // pausing reads cannot stop messages from other publishers
                return false;
            }
            c.writer.arrayHeader(3);
            c.writer.bulkString("message");
            c.writer.bulkString(channel);
            c.writer.bulkString(message);
            c.writer.flush(); // pendingBytes must include the RespWriter buffer
            schedule(c); // idle subscribers need output even without a new read event
            return true;
        } catch (IOException e) {
            close(c);
            return false;
        }
    }

    private void close(Connection c) {
        if (c.closed) return;
        c.closed = true;
        pubsub.remove(c);
        c.key.cancel();
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