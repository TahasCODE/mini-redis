package com.example.miniredis.store;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ExpirySweeper implements AutoCloseable {

    private static final int BATCH_SIZE = 1_000;
    private static final long MAX_BUSY_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final Store store;
    private final long intervalMillis;
    private final ScheduledExecutorService scheduler;

    public ExpirySweeper(Store store, long intervalMillis) {
        this.store = store;
        this.intervalMillis = intervalMillis;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "expiry-sweeper");
            thread.setDaemon(true);   // never keeps the JVM alive on its own
            return thread;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::runSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }


    private void runSafely() {
        try {
            sweepOnce();
        } catch (Throwable t) {
            System.err.println("expiry sweeper error: " + t);
        }
    }

    void sweepOnce() {
        long deadline = System.nanoTime() + MAX_BUSY_NANOS;
        int removed;
        do {
            removed = store.removeExpired(BATCH_SIZE);
        } while (removed == BATCH_SIZE && System.nanoTime() < deadline);
    }
}