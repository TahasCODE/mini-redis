package com.example.miniredis.persistence;

import com.example.miniredis.command.CommandRouter;
import com.example.miniredis.protocol.ProtocolException;
import com.example.miniredis.protocol.RespParser;
import com.example.miniredis.protocol.RespWriter;
import com.example.miniredis.store.Store;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


public final class AofLoader {

    private AofLoader() {
    }


    public static long load(Path path, Store store, CommandRouter router) throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long fileLength = Files.size(path);
        long lastGood = 0;          // the end of the last complete record
        long replayed = 0;
        List<List<String>> transaction = null;   // the commands of a MULTI whose EXEC we have not seen yet
        RespWriter sink = new RespWriter(OutputStream.nullOutputStream());   // replies are not needed: throw them away

        store.setClock(System.currentTimeMillis());
        try (InputStream in = new FileInputStream(path.toFile())) {
            RespParser parser = new RespParser(in);
            while (true) {
                List<String> command;
                try {
                    command = parser.readCommand();
                } catch (ProtocolException e) {
                    throw new IOException("the append-only file is damaged near byte " + parser.position()
                            + " (" + e.getMessage() + "). Repair or delete " + path + " and start again.", e);
                }
                if (command == null) {
                    break;                          // end of file, or a record that was cut off
                }
                if (command.isEmpty()) {
                    continue;
                }
                String first = command.get(0);
                if (first.startsWith("#TS:")) {
                    store.setClock(parseTime(first, parser.position()));
                    continue;                       // a time stamp alone is not yet a complete record
                }
                if (first.equalsIgnoreCase("MULTI")) {
                    if (transaction != null) {
                        throw damaged(parser, path, "MULTI inside a transaction");
                    }
                    transaction = new ArrayList<>();    // collect, do not run yet
                    continue;
                }
                if (first.equalsIgnoreCase("EXEC")) {
                    if (transaction == null) {
                        throw damaged(parser, path, "EXEC without MULTI");
                    }
                    for (List<String> queued : transaction) {   // the block is complete: run it
                        router.execute(queued, sink);
                        replayed++;
                    }
                    transaction = null;
                    lastGood = parser.position();
                    continue;
                }
                if (transaction != null) {
                    transaction.add(command);           // part of a transaction: wait for its EXEC
                    continue;
                }
                router.execute(command, sink);
                replayed++;
                lastGood = parser.position();       // this record is complete
            }
        } finally {
            store.useRealClock();
        }

        if (lastGood < fileLength) {
            System.out.println("append-only file: removing a half-written record or transaction at the end ("
                    + (fileLength - lastGood) + " bytes)");
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                channel.truncate(lastGood);
            }
        }
        return replayed;
    }

    private static IOException damaged(RespParser parser, Path path, String why) {
        return new IOException("the append-only file is damaged near byte " + parser.position() + " (" + why
                + "). Repair or delete " + path + " and start again.");
    }

    private static long parseTime(String annotation, long position) throws IOException {
        try {
            return Long.parseLong(annotation.substring("#TS:".length()));
        } catch (NumberFormatException e) {
            throw new IOException("bad time stamp in the append-only file near byte " + position, e);
        }
    }
}