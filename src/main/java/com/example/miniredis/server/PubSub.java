package com.example.miniredis.server;

import java.io.IOException;
import java.util.*;

final class PubSub {
    private final Map<String, Set<Connection>> channels = new HashMap<>();

    @FunctionalInterface
    interface Delivery {
        boolean send(Connection connection, String channel, String message);
    }

    void subscribe(Connection c, List<String> args) throws IOException {
        if (args.size() < 2) {
            c.writer.error("ERR wrong number of arguments for 'subscribe' command");
            return;
        }
        for (String channel : args.subList(1, args.size())) {
            c.subscriptions.add(channel);
            channels.computeIfAbsent(channel, ignored -> new LinkedHashSet<>()).add(c);
            acknowledge(c, "subscribe", channel);
        }
    }

    void unsubscribe(Connection c, List<String> args) throws IOException {
        List<String> requested = args.size() == 1
                ? new ArrayList<>(c.subscriptions) : args.subList(1, args.size());
        if (requested.isEmpty()) {
            acknowledge(c, "unsubscribe", null);
            return;
        }
        for (String channel : requested) {
            removeChannel(c, channel);
            acknowledge(c, "unsubscribe", channel);
        }
    }

    int publish(String channel, String message, Delivery delivery) {
        Set<Connection> subscribers = channels.get(channel);
        if (subscribers == null) return 0;
        int recipients = 0;
        // Delivery may close a slow client and remove it from the original set.
        for (Connection c : new ArrayList<>(subscribers)) {
            if (delivery.send(c, channel, message)) recipients++;
        }
        return recipients;
    }

    void remove(Connection c) {
        for (String channel : new ArrayList<>(c.subscriptions)) removeChannel(c, channel);
    }

    private void removeChannel(Connection c, String channel) {
        c.subscriptions.remove(channel);
        Set<Connection> subscribers = channels.get(channel);
        if (subscribers != null) {
            subscribers.remove(c);
            if (subscribers.isEmpty()) channels.remove(channel);
        }
    }

    private void acknowledge(Connection c, String action, String channel) throws IOException {
        c.writer.arrayHeader(3);
        c.writer.bulkString(action);
        c.writer.bulkString(channel);
        c.writer.integer(c.subscriptions.size());
    }
}
