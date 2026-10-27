package com.example.miniredis.store;

import java.util.concurrent.ConcurrentHashMap;


public final class Store {

    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();

    public void set(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    public boolean delete(String key) {
        return data.remove(key) != null;
    }

    public boolean exists(String key) {
        return data.containsKey(key);
    }

    public int size() {
        return data.size();
    }

    public void clear() {
        data.clear();
    }
}