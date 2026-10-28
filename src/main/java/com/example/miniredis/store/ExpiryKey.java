package com.example.miniredis.store;

record ExpiryKey(long expiresAt, String key) implements Comparable<ExpiryKey> {

    @Override
    public int compareTo(ExpiryKey other) {
        int byTime = Long.compare(expiresAt, other.expiresAt);
        return byTime != 0 ? byTime : key.compareTo(other.key);
    }
}