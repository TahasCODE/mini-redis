package com.example.miniredis.store;


record Entry(Value value, long expiresAt) {

    boolean isExpired(long nowMillis) {
        return expiresAt != Store.NO_EXPIRY && nowMillis >= expiresAt;
    }
}