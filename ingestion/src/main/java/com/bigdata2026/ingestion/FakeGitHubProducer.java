package com.bigdata2026.ingestion;

import com.bigdata2026.schema.GitHubEventSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public final class FakeGitHubProducer {
    private static final String[] EVENT_TYPES = {
        "PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent"
    };
    private static final String[] ACTORS = {"alice", "bob", "charlie", "diana", "eve"};
    private static final String[] REPOS  = {"org/alpha", "org/beta", "org/gamma", "org/delta"};

    private FakeGitHubProducer() {}

    public static void run() throws InterruptedException {
        String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");

        // Kafka producer configuration:
        //   bootstrap.servers — one or more brokers to bootstrap the connection
        //   key.serializer    — how to convert the message key to bytes
        //   value.serializer  — how to convert the message value to bytes
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Random rng = new Random();

        // try-with-resources ensures the producer is flushed and closed on exit
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("[fake-producer] Connected to %s, publishing to topic '%s'%n", broker, GitHubEventSchema.TOPIC);
            while (true) {
                String type  = EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)];
                String actor = ACTORS[rng.nextInt(ACTORS.length)];
                String repo  = REPOS[rng.nextInt(REPOS.length)];
                String json  = String.format(
                    "{\"%s\":\"%s\",\"%s\":{\"%s\":\"%s\"},\"%s\":{\"%s\":\"%s\"},\"%s\":\"%s\"}",
                    GitHubEventSchema.F_TYPE, type,
                    GitHubEventSchema.F_ACTOR, GitHubEventSchema.F_LOGIN, actor,
                    GitHubEventSchema.F_REPO, GitHubEventSchema.F_REPO_NAME, repo,
                    GitHubEventSchema.F_CREATED_AT, java.time.Instant.now()
                );

                // ProducerRecord(topic, key, value)
                // The key is used for partition routing — same key always goes to the same partition.
                // send() is async: it batches internally and returns a Future.
                producer.send(new ProducerRecord<>(GitHubEventSchema.TOPIC, actor, json));
                System.out.println("[fake-producer] sent: " + json);
                Thread.sleep(500);
            }
        }
    }
}
