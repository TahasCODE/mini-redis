package com.example.miniredis.command;

import com.example.miniredis.protocol.RespWriter;

import java.io.IOException;
import java.util.List;

@FunctionalInterface
public interface CommandHandler {

    void handle(List<String> args, RespWriter out) throws IOException;
}