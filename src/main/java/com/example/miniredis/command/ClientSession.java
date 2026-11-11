package com.example.miniredis.command;

import java.util.ArrayList;
import java.util.List;

public final class ClientSession {


    public boolean inTransaction() { return inMulti; }

    boolean inMulti;

    boolean dirty;

    final List<List<String>> queued = new ArrayList<>();

    void startTransaction() {
        inMulti = true;
        dirty = false;
        queued.clear();
    }

    void reset() {
        inMulti = false;
        dirty = false;
        queued.clear();
    }
}