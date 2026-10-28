package com.example.miniredis.store;

public class StoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String code;

    public StoreException(String message) {
        this("ERR", message);
    }

    public StoreException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String reply() {
        return code + " " + getMessage();
    }
}