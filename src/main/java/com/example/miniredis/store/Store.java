package com.example.miniredis.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


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
            // a plain SET also clears any old expiry
            return swap(k, old, new Entry(new StringValue(value), expiresAt));
        });
        return written[0];
    }


    public String get(String key) {
        Entry e = live(key, System.currentTimeMillis());
        if (e == null) {
            return null;
        }
        if (e.value() instanceof StringValue s) {
            return s.text();
        }
        throw wrongType();
    }

    public boolean exists(String key) {
        return live(key, System.currentTimeMillis()) != null;
    }

    public String type(String key) {
        Entry e = live(key, System.currentTimeMillis());
        if (e == null) {
            return "none";
        }
        return e.value() instanceof ListValue ? "list" : "string";
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
        index.clear();   // an entry that slips in during a clear is harmless: removeExpired double-checks
    }

    // ------------------------------------------------------------------ counters


    public long increment(String key, long delta) {
        long now = System.currentTimeMillis();
        long[] result = new long[1];
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            long current = 0;
            if (exists) {
                if (!(old.value() instanceof StringValue text)) {
                    throw wrongType();
                }
                current = parseInteger(text.text());
            }
            long expiresAt = exists ? old.expiresAt() : NO_EXPIRY;
            long next;
            try {
                next = Math.addExact(current, delta);
            } catch (ArithmeticException e) {
                throw new StoreException("increment or decrement would overflow");
            }
            result[0] = next;
            return swap(k, old, new Entry(new StringValue(Long.toString(next)), expiresAt));
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

    // ------------------------------------------------------------------ lists


    public long push(String key, List<String> values, boolean atFront) {
        long now = System.currentTimeMillis();
        long[] length = new long[1];
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            ListValue list;
            Entry next;
            if (exists) {
                if (!(old.value() instanceof ListValue existing)) {
                    throw wrongType();
                }
                list = existing;
                next = old;
            } else {
                list = new ListValue();
                next = new Entry(list, NO_EXPIRY);
            }
            for (String value : values) {
                if (atFront) {
                    list.items.addFirst(value);
                } else {
                    list.items.addLast(value);
                }
            }
            length[0] = list.items.size();
            return swap(k, old, next);
        });
        return length[0];
    }


    public List<String> pop(String key, long count, boolean fromFront) {
        long now = System.currentTimeMillis();
        AtomicReference<List<String>> removed = new AtomicReference<>();
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return swap(k, old, null);
            }
            if (!(old.value() instanceof ListValue list)) {
                throw wrongType();
            }
            List<String> taken = new ArrayList<>();
            while (taken.size() < count && !list.items.isEmpty()) {
                taken.add(fromFront ? list.items.pollFirst() : list.items.pollLast());
            }
            removed.set(taken);
            return list.items.isEmpty() ? swap(k, old, null) : old;   // an empty list does not exist in Redis
        });
        return removed.get();
    }


    public long listLength(String key) {
        return readList(key, list -> (long) list.items.size(), 0L);
    }


    public List<String> range(String key, long start, long stop) {
        return readList(key, list -> {
            long length = list.items.size();
            long from = start < 0 ? start + length : start;
            long to = stop < 0 ? stop + length : stop;
            if (from < 0) {
                from = 0;
            }
            if (to >= length) {
                to = length - 1;
            }
            List<String> result = new ArrayList<>();
            if (from > to || from >= length) {
                return result;
            }
            long position = 0;
            for (String item : list.items) {
                if (position > to) {
                    break;
                }
                if (position >= from) {
                    result.add(item);
                }
                position++;
            }
            return result;
        }, new ArrayList<>());
    }

    public String index(String key, long position) {
        return readList(key, list -> {
            long length = list.items.size();
            long wanted = position < 0 ? position + length : position;
            if (wanted < 0 || wanted >= length) {
                return null;
            }
            long current = 0;
            for (String item : list.items) {
                if (current == wanted) {
                    return item;
                }
                current++;
            }
            return null;
        }, null);
    }


    private <T> T readList(String key, Function<ListValue, T> action, T ifMissing) {
        long now = System.currentTimeMillis();
        AtomicReference<T> result = new AtomicReference<>(ifMissing);   // just a box the lambda can put the answer in
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return swap(k, old, null);
            }
            if (!(old.value() instanceof ListValue list)) {
                throw wrongType();
            }
            result.set(action.apply(list));
            return old;
        });
        return result.get();
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

    private static StoreException wrongType() {
        return new StoreException("WRONGTYPE", "Operation against a key holding the wrong kind of value");
    }

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
            return next;                           // same expiry (or none): the index is already right
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