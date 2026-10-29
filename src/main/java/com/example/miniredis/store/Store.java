package com.example.miniredis.store;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        return switch (e.value()) {
            case StringValue s -> "string";
            case ListValue l -> "list";
            case HashValue h -> "hash";
        };
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
            long[] bounds = bounds(list.items.size(), start, stop);
            List<String> result = new ArrayList<>();
            if (bounds == null) {
                return result;
            }
            long from = bounds[0];
            long to = bounds[1];
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


    public void trim(String key, long start, long stop) {
        long now = System.currentTimeMillis();
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return swap(k, old, null);
            }
            if (!(old.value() instanceof ListValue list)) {
                throw wrongType();
            }
            long length = list.items.size();
            long[] bounds = bounds(length, start, stop);
            if (bounds == null) {
                return swap(k, old, null);            // nothing survives: an empty list does not exist
            }
            for (long i = 0; i < bounds[0]; i++) {
                list.items.pollFirst();
            }
            for (long i = bounds[1] + 1; i < length; i++) {
                list.items.pollLast();
            }
            return old;
        });
    }

    private static long[] bounds(long length, long start, long stop) {
        long from = start < 0 ? start + length : start;
        long to = stop < 0 ? stop + length : stop;
        if (from < 0) {
            from = 0;
        }
        if (to >= length) {
            to = length - 1;
        }
        if (from > to || from >= length) {
            return null;
        }
        return new long[] {from, to};
    }


    private <T> T readList(String key, Function<ListValue, T> action, T ifMissing) {
        return readValue(key, ListValue.class, action, ifMissing);
    }

    private <T> T readHash(String key, Function<HashValue, T> action, T ifMissing) {
        return readValue(key, HashValue.class, action, ifMissing);
    }

    private <V extends Value, T> T readValue(String key, Class<V> wanted, Function<V, T> action, T ifMissing) {
        long now = System.currentTimeMillis();
        AtomicReference<T> result = new AtomicReference<>(ifMissing);   // a box the lambda puts the answer in
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return swap(k, old, null);
            }
            if (!wanted.isInstance(old.value())) {
                throw wrongType();
            }
            result.set(action.apply(wanted.cast(old.value())));
            return old;
        });
        return result.get();
    }

    // ------------------------------------------------------------------ hashes


    public long hashSet(String key, List<String> pairs) {
        long now = System.currentTimeMillis();
        long[] added = new long[1];
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            HashValue hash;
            Entry next;
            if (exists) {
                if (!(old.value() instanceof HashValue existing)) {
                    throw wrongType();
                }
                hash = existing;
                next = old;
            } else {
                hash = new HashValue();
                next = new Entry(hash, NO_EXPIRY);
            }
            for (int i = 0; i < pairs.size(); i += 2) {
                if (hash.fields.put(pairs.get(i), pairs.get(i + 1)) == null) {
                    added[0]++;
                }
            }
            return swap(k, old, next);
        });
        return added[0];
    }

    public String hashGet(String key, String field) {
        return readHash(key, hash -> hash.fields.get(field), null);
    }

    public Map<String, String> hashGetAll(String key) {
        return readHash(key, hash -> new LinkedHashMap<>(hash.fields), new LinkedHashMap<>());
    }

    public long hashLength(String key) {
        return readHash(key, hash -> (long) hash.fields.size(), 0L);
    }

    public long hashDelete(String key, List<String> fields) {
        long now = System.currentTimeMillis();
        long[] removed = new long[1];
        data.computeIfPresent(key, (k, old) -> {
            if (old.isExpired(now)) {
                return swap(k, old, null);
            }
            if (!(old.value() instanceof HashValue hash)) {
                throw wrongType();
            }
            for (String field : fields) {
                if (hash.fields.remove(field) != null) {
                    removed[0]++;
                }
            }
            return hash.fields.isEmpty() ? swap(k, old, null) : old;
        });
        return removed[0];
    }

    public long hashIncrement(String key, String field, long delta) {
        long now = System.currentTimeMillis();
        long[] result = new long[1];
        data.compute(key, (k, old) -> {
            boolean exists = old != null && !old.isExpired(now);
            HashValue hash;
            Entry next;
            if (exists) {
                if (!(old.value() instanceof HashValue existing)) {
                    throw wrongType();
                }
                hash = existing;
                next = old;
            } else {
                hash = new HashValue();
                next = new Entry(hash, NO_EXPIRY);
            }
            long current = 0;
            String text = hash.fields.get(field);
            if (text != null) {
                try {
                    current = parseInteger(text);
                } catch (StoreException e) {
                    throw new StoreException("hash value is not an integer");
                }
            }
            long sum;
            try {
                sum = Math.addExact(current, delta);
            } catch (ArithmeticException e) {
                throw new StoreException("increment or decrement would overflow");
            }
            hash.fields.put(field, Long.toString(sum));    // only reached when nothing failed
            result[0] = sum;
            return swap(k, old, next);
        });
        return result[0];
    }

    // ------------------------------------------------------------------ looking at keys

    public record ScanResult(long nextCursor, List<String> keys) {
    }

    private record Candidate(long hash, String key, boolean live) {
    }

    public List<String> keys(String pattern) {
        long now = System.currentTimeMillis();
        List<String> found = new ArrayList<>();
        for (Map.Entry<String, Entry> item : data.entrySet()) {     // safe to walk while others change the map
            if (!item.getValue().isExpired(now) && GlobMatcher.matches(pattern, item.getKey())) {
                found.add(item.getKey());
            }
        }
        return found;
    }

    public ScanResult scan(long cursor, int count, String pattern) {
        long now = System.currentTimeMillis();
        List<Candidate> candidates = new ArrayList<>();
        for (Map.Entry<String, Entry> item : data.entrySet()) {
            long hash = item.getKey().hashCode() & 0xFFFFFFFFL;     // the key's fixed place, 0 .. 2^32-1
            if (hash >= cursor) {
                candidates.add(new Candidate(hash, item.getKey(), !item.getValue().isExpired(now)));
            }
        }
        candidates.sort(Comparator.comparingLong(Candidate::hash).thenComparing(Candidate::key));
        int end = Math.min(count, candidates.size());
        while (end < candidates.size() && end > 0 && candidates.get(end).hash() == candidates.get(end - 1).hash()) {
            end++;                                                   // finish the group of equal numbers
        }
        List<String> page = new ArrayList<>();
        for (Candidate c : candidates.subList(0, end)) {
            if (c.live() && (pattern == null || GlobMatcher.matches(pattern, c.key()))) {
                page.add(c.key());
            }
        }
        long next = end < candidates.size() ? candidates.get(end).hash() : 0;
        return new ScanResult(next, page);
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