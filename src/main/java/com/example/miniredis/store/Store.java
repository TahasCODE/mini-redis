package com.example.miniredis.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public final class Store {

    public static final long NO_EXPIRY = -1;

    public enum SetCondition { ALWAYS, ONLY_IF_ABSENT, ONLY_IF_PRESENT }

    private static final String NOT_INTEGER = "value is not an integer or out of range";

    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();

    private final ConcurrentSkipListSet<ExpiryKey> index = new ConcurrentSkipListSet<>();

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
                return swap(k, old, null);   // XX: key missing, write nothing (and drop a stale expired entry)
            }
            written[0] = true;
            return swap(k, old, new Entry(value, expiresAt));   // a plain SET also clears any old expiry
        });
        return written[0];
    }

    /** @return the value, or null if the key does not exist or has expired. */
    public String get(String key) {
        Entry e = live(key, System.currentTimeMillis());
        return e == null ? null : e.value();
    }

    public boolean exists(String key) {
        return live(key, System.currentTimeMillis()) != null;
    }

    public boolean delete(String key) {
        long now = System.currentTimeMillis();
        boolean[] wasLive = {false};
        data.compute(key, (k, old) -> {
            wasLive[0] = old != null && !old.isExpired(now);
            return swap(k, old, null);
        });
        return wasLive[0];
    }

    public int size() {
        return data.size();
    }

    public void clear() {
        data.clear();
        index.clear();
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
            return swap(k, old, new Entry(Long.toString(next), expiresAt));
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
                return swap(k, old, null);
            }
            changed[0] = true;
            return swap(k, old, new Entry(old.value(), expiresAt));
        });
        return changed[0];
    }

    /** @return -2 if the key does not exist, -1 if it has no expiry, otherwise the milliseconds left. */
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
                return swap(k, old, null);
            }
            if (old.expiresAt() == NO_EXPIRY) {
                return old;
            }
            changed[0] = true;
            return swap(k, old, new Entry(old.value(), NO_EXPIRY));
        });
        return changed[0];
    }


    public int removeExpired(int limit) {
        long now = System.currentTimeMillis();
        int[] deleted = {0};
        int examined = 0;
        for (ExpiryKey due : index) {            // ascending order: earliest due time first
            if (due.expiresAt() > now || examined >= limit) {
                break;                            // everything after this is later still
            }
            examined++;
            index.remove(due);
            data.computeIfPresent(due.key(), (k, current) -> {
                if (current.isExpired(now)) {     // the index line may be stale: always check the real entry
                    deleted[0]++;
                    return swap(k, current, null);
                }
                return current;
            });
        }
        return deleted[0];
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
        // remove only if nobody replaced it in the meantime
        data.computeIfPresent(key, (k, current) -> current == e ? swap(k, current, null) : current);
        return null;
    }


    private Entry swap(String key, Entry old, Entry next) {
        long oldAt = old == null ? NO_EXPIRY : old.expiresAt();
        long nextAt = next == null ? NO_EXPIRY : next.expiresAt();
        if (oldAt == nextAt) {
            return next;
        }
        if (oldAt != NO_EXPIRY) {
            index.remove(new ExpiryKey(oldAt, key));
        }
        if (nextAt != NO_EXPIRY) {
            index.add(new ExpiryKey(nextAt, key));
        }
        return next;
    }
}