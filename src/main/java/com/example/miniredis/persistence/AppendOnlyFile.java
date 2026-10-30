package com.example.miniredis.persistence;

import com.example.miniredis.store.Store;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;


public final class AppendOnlyFile implements AutoCloseable {

    public enum FsyncPolicy {
        ALWAYS,
        EVERYSEC,
        NO;

        public static FsyncPolicy parse(String text) {
            return switch (text.toLowerCase(Locale.ROOT)) {
                case "always" -> ALWAYS;
                case "everysec" -> EVERYSEC;
                case "no" -> NO;
                default -> throw new IllegalArgumentException("fsync policy must be always, everysec or no, not '" + text + "'");
            };
        }
    }

    public record RewriteResult(long keys, long commands, long bytesBefore, long bytesAfter, long millis) {
    }

    private final Path path;
    private final FsyncPolicy policy;
    private final long autoRewriteMinBytes;

    private volatile FileOutputStream out;
    private volatile boolean dirty;          // written but not yet fsynced (EVERYSEC only)
    private volatile boolean closed;
    private long size;                       // bytes in the file; guarded by "this"
    private long baseSize;                   // size right after the last rewrite (or at start-up)
    private Thread syncer;

    private AppendOnlyFile(Path path, FsyncPolicy policy, long autoRewriteMinBytes, long size) throws IOException {
        this.path = path;
        this.policy = policy;
        this.autoRewriteMinBytes = autoRewriteMinBytes;
        this.size = size;
        this.baseSize = size;
        this.out = new FileOutputStream(path.toFile(), true);
    }


    public static AppendOnlyFile open(Path path, FsyncPolicy policy, long autoRewriteMinBytes) throws IOException {
        if (Files.deleteIfExists(tempPath(path))) {
            System.out.println("append-only file: removed an unfinished rewrite (" + tempPath(path).getFileName() + ")");
        }
        long size = Files.exists(path) ? Files.size(path) : 0;
        AppendOnlyFile file = new AppendOnlyFile(path, policy, autoRewriteMinBytes, size);
        if (policy == FsyncPolicy.EVERYSEC) {
            file.syncer = new Thread(file::syncLoop, "aof-fsync");
            file.syncer.setDaemon(true);
            file.syncer.start();
        }
        return file;
    }

    public FsyncPolicy policy() {
        return policy;
    }


    public synchronized void append(long atMillis, List<String> command) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        writeAscii(record, "#TS:" + atMillis + "\r\n");
        encodeCommand(record, command);
        byte[] bytes = record.toByteArray();
        FileOutputStream stream = out;
        stream.write(bytes);                    // one call: the whole record or (after a crash) a cut-off record
        size += bytes.length;
        if (policy == FsyncPolicy.ALWAYS) {
            stream.getFD().sync();              // do not answer the client until the disk has it
        } else {
            dirty = true;
        }
    }

    public synchronized boolean shouldRewrite() {
        return autoRewriteMinBytes > 0 && size >= autoRewriteMinBytes && size >= 2 * baseSize;
    }


    public synchronized RewriteResult rewrite(Store store) throws IOException {
        long started = System.nanoTime();
        long bytesBefore = size;
        Path temp = tempPath(path);
        long now = store.now();
        long keys = 0;
        long commands = 0;

        try (FileOutputStream raw = new FileOutputStream(temp.toFile());
             OutputStream buffered = new BufferedOutputStream(raw, 1 << 16)) {
            ByteArrayOutputStream chunk = new ByteArrayOutputStream();
            writeAscii(chunk, "#TS:" + now + "\r\n");           // the loader's clock for everything below
            buffered.write(chunk.toByteArray());
            for (String key : store.keys("*")) {
                List<List<String>> rebuild = store.rebuildCommands(key, now);
                if (rebuild.isEmpty()) {
                    continue;                                    // deleted or expired while we were walking
                }
                keys++;
                for (List<String> command : rebuild) {
                    chunk.reset();
                    encodeCommand(chunk, command);
                    buffered.write(chunk.toByteArray());
                    commands++;
                }
            }
            buffered.flush();
            raw.getFD().sync();                                  // the helper file is on the disk BEFORE the swap
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(temp);                          // the old file is untouched: just give up
            throw e;
        }

        // The swap. The old stream must be closed first (Windows refuses to replace an open file).
        out.close();
        try {
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            out = new FileOutputStream(path.toFile(), true);     // keep going with the old file
            Files.deleteIfExists(temp);
            throw e;
        }
        out = new FileOutputStream(path.toFile(), true);
        size = Files.size(path);
        baseSize = size;
        dirty = false;                                           // the new file was fsynced before the rename
        return new RewriteResult(keys, commands, bytesBefore, size, (System.nanoTime() - started) / 1_000_000);
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (syncer != null) {
            syncer.interrupt();
        }
        try {
            if (policy != FsyncPolicy.ALWAYS) {
                out.getFD().sync();     // a clean shutdown loses nothing, whatever the policy
            }
        } finally {
            out.close();
        }
    }

    /** The EVERYSEC thread: once per second, force the data to disk if anything was written. */
    private void syncLoop() {
        while (!closed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            if (dirty) {
                dirty = false;          // cleared BEFORE the sync: a write during the sync marks it dirty again
                try {
                    out.getFD().sync();     // not under the lock: writers keep going while the disk works
                } catch (IOException e) {
                    // The stream was just replaced by a rewrite, which fsynced its file. Nothing to do.
                }
            }
        }
    }

    private static Path tempPath(Path path) {
        return path.resolveSibling(path.getFileName() + ".rewrite.tmp");
    }

    private static void encodeCommand(ByteArrayOutputStream target, List<String> command) {
        writeAscii(target, "*" + command.size() + "\r\n");
        for (String word : command) {
            byte[] bytes = word.getBytes(StandardCharsets.UTF_8);
            writeAscii(target, "$" + bytes.length + "\r\n");
            target.write(bytes, 0, bytes.length);
            writeAscii(target, "\r\n");
        }
    }

    private static void writeAscii(ByteArrayOutputStream target, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        target.write(bytes, 0, bytes.length);
    }
}