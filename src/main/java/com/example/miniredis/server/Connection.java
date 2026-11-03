package com.example.miniredis.server;

import com.example.miniredis.command.ClientSession;
import com.example.miniredis.protocol.RespWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

final class Connection {

    final SocketChannel channel;
    final String name;
    final ClientSession session = new ClientSession();

    private byte[] input = new byte[4096];
    private int inputStart;     // start of the bytes not yet used
    private int inputEnd;       // end of the received bytes

    private final ByteArrayOutputStream replies = new ByteArrayOutputStream();
    final RespWriter writer = new RespWriter(replies);
    private ByteBuffer sending;

    boolean closeAfterWrite;

    Connection(SocketChannel channel, String name) {
        this.channel = channel;
        this.name = name;
    }

    // ---------- input ----------

    boolean readFromChannel(ByteBuffer scratch) throws IOException {
        scratch.clear();
        int n = channel.read(scratch);
        if (n < 0) {
            return false;
        }
        scratch.flip();
        compactInput(n);
        scratch.get(input, inputEnd, n);
        inputEnd += n;
        return true;
    }

    private void compactInput(int extra) {
        int used = inputEnd - inputStart;
        if (used == 0) {
            inputStart = 0;
            inputEnd = 0;
            if (input.length > 256 * 1024) {
                input = new byte[4096];          // give back the memory of a big value
            }
        } else if (inputStart > 0 && inputEnd + extra > input.length) {
            System.arraycopy(input, inputStart, input, 0, used);   // move the unused bytes to the front
            inputStart = 0;
            inputEnd = used;
        }
        if (inputEnd + extra > input.length) {
            input = Arrays.copyOf(input, Math.max(input.length * 2, inputEnd + extra));   // still too small: grow
        }
    }

    byte[] inputBytes() { return input; }
    int inputStart() { return inputStart; }
    int inputEnd() { return inputEnd; }
    void consume(int bytes) { inputStart += bytes; }
    int inputSize() { return inputEnd - inputStart; }

    // ---------- output ----------

    int pendingBytes() {
        return replies.size() + (sending == null ? 0 : sending.remaining());
    }


    boolean flush() throws IOException {
        writer.flush();
        while (true) {
            if (sending == null || !sending.hasRemaining()) {
                if (replies.size() == 0) {
                    sending = null;
                    return true;
                }
                sending = ByteBuffer.wrap(replies.toByteArray());
                replies.reset();
            }
            channel.write(sending);
            if (sending.hasRemaining()) {
                return false;                    // the network buffer is full: wait until it has room
            }
        }
    }
}