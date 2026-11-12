package com.example.miniredis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.protocol.ProtocolVersion;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class LettuceCompatibilityCheck {
    public static void main(String[] args) throws InterruptedException {
        var uri = RedisURI.Builder.redis("127.0.0.1", 6380)
                .withTimeout(Duration.ofSeconds(5))
                .build();
        var client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .protocolVersion(ProtocolVersion.RESP2)
                .autoReconnect(false)
                .build());

        String prefix = "day12:" + UUID.randomUUID();
        String counter = prefix + ":counter";
        String list = prefix + ":list";
        String hash = prefix + ":hash";
        String channel = prefix + ":news";

        try (var connection = client.connect()) {
            var redis = connection.sync();

            try {
                check("PING", "PONG", redis.ping());
                check("SET", "OK", redis.set(counter, "41"));
                check("GET", "41", redis.get(counter));
                check("INCR", 42L, redis.incr(counter));

                check("RPUSH", 2L, redis.rpush(list, "first", "second"));
                check("LRANGE", List.of("first", "second"), redis.lrange(list, 0, -1));

                check("HSET", true, redis.hset(hash, "name", "MiniRedis"));
                check("HGET", "MiniRedis", redis.hget(hash, "name"));

                redis.multi();
                redis.incr(counter);
                redis.get(counter);
                var result = redis.exec();

                check("EXEC reply count", 2, result.size());
                check("EXEC INCR result", 43L, result.get(0));
                check("EXEC GET result", "43", result.get(1));

                var messages = new LinkedBlockingQueue<String>();
                try (var subscriber = client.connectPubSub()) {
                    subscriber.addListener(new RedisPubSubAdapter<String, String>() {
                        @Override
                        public void message(String receivedChannel, String message) {
                            if (channel.equals(receivedChannel)) {
                                messages.offer(message);
                            }
                        }
                    });

                    subscriber.sync().subscribe(channel);
                    check("PUBLISH with subscriber", 1L, redis.publish(channel, "hello"));
                    check("Subscriber received message", "hello",
                            messages.poll(5, TimeUnit.SECONDS));

                    subscriber.sync().unsubscribe(channel);
                    check("PUBLISH after unsubscribe", 0L, redis.publish(channel, "ignored"));
                }
            } finally {
                redis.del(counter, list, hash);
            }
        } finally {
            client.shutdown();
        }

        System.out.println("RESULT: ALL OK");
    }

    private static void check(String label, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(label + ": expected " + expected + ", got " + actual);
        }
        System.out.println("PASS " + label);
    }
}