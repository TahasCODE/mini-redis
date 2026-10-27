package com.example.miniredis.store;

public class StoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StoreException(String message) {
        super(message);
    }
}