package com.example.miniredis.store;

import java.util.concurrent.ConcurrentHashMap;


public final class Store {

    public static final long NO_EXPIRY = -1;

    public enum SetCondition { ALWAYS, ONLY_IF_ABSENT, ONLY_IF_PRESENT }

    private static final String NOT_INTEGER = "value is not an integer or out of range";

    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------ strings


    public boolean set(String key, String value, long expiresAt, SetCondition condition) {
        long now = System.currentTimeMillis();
        boolean[] written = {false};
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            if (condition == SetCondition.ONLY_IF_ABSENT && exists) {
                return old;          // NX: key already there, change nothing
            }
            if (condition == SetCondition.ONLY_IF_PRESENT && !exists) {
                return null;         // XX: key missing, write nothing (and drop a stale expired entry)
            }
            written[0] = true;
            return new Entry(value, expiresAt);   // a plain SET also clears any old expiry
        });
        return written[0];
    }

    public String get(String key) {
        Entry e = live(key, System.currentTimeMillis());
        return e == null ? null : e.value();
    }

    public boolean exists(String key) {
        return live(key, System.currentTimeMillis()) != null;
    }

    public boolean delete(String key) {
        long now = System.currentTimeMillis();
        Entry removed = data.remove(key);
        return removed != null && !removed.isExpired(now);
    }

    public int size() {
        return data.size();
    }

    public void clear() {
        data.clear();
    }

    // ------------------------------------------------------------------ counters


    public long increment(String key, long delta) {
        long now = System.currentTimeMillis();
        long[] result = new long[1];
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            long current = exists ? parseInteger(old.value()) : 0;
            long expiresAt = exists ? old.expiresAt() : NO_EXPIRY;
            long next;
            try {
                next = Math.addExact(current, delta);
            } catch (ArithmeticException e) {
                throw new StoreException("increment or decrement would overflow");
            }
            result[0] = next;
            return new Entry(Long.toString(next), expiresAt);
        });
        return result[0];
    }

    public static long parseInteger(String text) {
        if (!text.matches("0|-?[1-9][0-9]*")) {
            throw new StoreException(NOT_INTEGER);
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new StoreException(NOT_INTEGER);
        }
    }

    // ------------------------------------------------------------------ expiry

    public boolean setExpiry(String key, long expiresAt) {
        long now = System.currentTimeMillis();
        boolean[] changed = {false};
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return null;
            }
            changed[0] = true;
            return new Entry(old.value(), expiresAt);
        });
        return changed[0];
    }

    public long ttlMillis(String key) {
        long now = System.currentTimeMillis();
        Entry e = live(key, now);
        if (e == null) {
            return -2;
        }
        if (e.expiresAt() == NO_EXPIRY) {
            return -1;
        }
        return Math.max(0, e.expiresAt() - now);
    }

    public boolean persist(String key) {
        long now = System.currentTimeMillis();
        boolean[] changed = {false};
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return null;
            }
            if (old.expiresAt() == NO_EXPIRY) {
                return old;
            }
            changed[0] = true;
            return new Entry(old.value(), NO_EXPIRY);
        });
        return changed[0];
    }

    // ------------------------------------------------------------------ helpers

    private Entry live(String key, long now) {
        Entry e = data.get(key);
        if (e == null) {
            return null;
        }
        if (!e.isExpired(now)) {
            return e;
        }
        data.remove(key, e);   // remove only if nobody replaced it in the meantime
        return null;
    }
}