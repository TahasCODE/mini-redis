package com.example.miniredis.protocol;

import java.io.IOException;

/** Thrown when a client sends bytes that do not follow the RESP protocol. */
public class ProtocolException extends IOException {

    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }
}