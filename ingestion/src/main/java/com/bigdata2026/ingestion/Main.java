package com.bigdata2026.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Main — GitHub Activity Kafka Producer
 * ======================================
 * CS523 BDT Final Project — Real-Time GitHub Open Source Trend Analytics
 *
 * Polls the GitHub public Events API on a configurable interval,
 * enriches each event, and wraps it in the shared Event envelope:
 *
 *   { "id": "<github_event_id>",
 *     "source": "github",
 *     "payload": "<enriched github JSON as escaped string>",
 *     "timestamp": <epoch_ms> }
 *
 * Configuration via environment variables:
 *   GITHUB_TOKEN              GitHub Personal Access Token (recommended)
 *                             Without token: 60 req/hr  → set POLL_INTERVAL=60
 *                             With    token: 5000 req/hr → POLL_INTERVAL=10
 *   KAFKA_BOOTSTRAP_SERVERS   Kafka broker(s)  (default: kafka:9092)
 *   KAFKA_TOPIC               Topic name       (default: github-events)
 *   POLL_INTERVAL             Seconds between polls (default: 10)
 *
 *   --- Optional: MSK SASL_SSL/SCRAM (leave unset for local plaintext Kafka) ---
 *   KAFKA_SECURITY_PROTOCOL   Set to SASL_SSL to enable MSK auth
 *   KAFKA_SASL_MECHANISM      e.g. SCRAM-SHA-512
 *   KAFKA_SASL_USERNAME       MSK SCRAM username
 *   KAFKA_SASL_PASSWORD       MSK SCRAM password
 */
public final class Main {

    // ── Config ────────────────────────────────────────────────────────────────
    private static final String GITHUB_TOKEN    = cfg("GITHUB_TOKEN", "");
    private static final String KAFKA_BOOTSTRAP = cfg("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String KAFKA_TOPIC     = cfg("KAFKA_TOPIC", "github-events");
    private static final int    POLL_INTERVAL   = Integer.parseInt(cfg("POLL_INTERVAL", "10"));

    // MSK SASL (optional)
    private static final String SECURITY_PROTOCOL = cfg("KAFKA_SECURITY_PROTOCOL", "");
    private static final String SASL_MECHANISM    = cfg("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512");
    private static final String SASL_USERNAME     = cfg("KAFKA_SASL_USERNAME", "");
    private static final String SASL_PASSWORD     = cfg("KAFKA_SASL_PASSWORD", "");

    private static final String GITHUB_EVENTS_URL =
        "https://api.github.com/events?per_page=100";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Trend weight per event type (used by downstream Spark SQL)
    private static final Map<String, Integer> EVENT_WEIGHTS = Map.ofEntries(
        Map.entry("PushEvent",                        2),
        Map.entry("WatchEvent",                       3),   // WatchEvent == star
        Map.entry("ForkEvent",                        2),
        Map.entry("IssuesEvent",                      1),
        Map.entry("PullRequestEvent",                 1),
        Map.entry("CreateEvent",                      1),
        Map.entry("ReleaseEvent",                     2),
        Map.entry("DeleteEvent",                      0),
        Map.entry("GollumEvent",                      0),
        Map.entry("MemberEvent",                      0),
        Map.entry("PublicEvent",                      0),
        Map.entry("CommitCommentEvent",               0),
        Map.entry("IssueCommentEvent",                0),
        Map.entry("PullRequestReviewEvent",           0),
        Map.entry("PullRequestReviewCommentEvent",    0)
    );

    private Main() {}

    // ── Main ─────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws InterruptedException {
        printBanner();

        KafkaProducer<String, String> producer = createProducer();
        Set<String> seenIds = new HashSet<>();
        int totalSent = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Ingestion] Shutdown received — flushing and closing...");
            producer.flush();
            producer.close();
            System.out.println("[Ingestion] Closed. Goodbye.");
        }));

        try {
            while (true) {
                List<JsonNode> events = fetchEvents();
                int newCount = 0;

                for (JsonNode raw : events) {
                    String eventId = raw.path("id").asText("");
                    if (eventId.isEmpty() || seenIds.contains(eventId)) continue;

                    seenIds.add(eventId);

                    try {
                        ObjectNode enriched  = enrichEvent(raw);
                        String     payloadJson = MAPPER.writeValueAsString(enriched);

                        // Wrap in the shared Event envelope expected by the streaming layer
                        ObjectNode envelope = MAPPER.createObjectNode();
                        envelope.put("id",        eventId);
                        envelope.put("source",    "github");
                        envelope.put("payload",   payloadJson);
                        envelope.put("timestamp", Instant.now().toEpochMilli());

                        String envelopeJson = MAPPER.writeValueAsString(envelope);
                        producer.send(
                            new ProducerRecord<>(KAFKA_TOPIC, eventId, envelopeJson),
                            (meta, ex) -> {
                                if (ex != null)
                                    System.err.println("[Ingestion] Send error: " + ex.getMessage());
                            }
                        );
                        newCount++;
                    } catch (Exception e) {
                        System.err.println("[Ingestion] Error processing event " + eventId + ": " + e.getMessage());
                    }
                }

                producer.flush();
                totalSent += newCount;

                if (newCount > 0)
                    System.out.printf("[Ingestion] Sent %d new events → %s. Session total: %d%n",
                        newCount, KAFKA_TOPIC, totalSent);
                else
                    System.out.println("[Ingestion] No new events this cycle.");

                // Trim dedup cache to avoid unbounded growth
                if (seenIds.size() > 50_000) {
                    System.out.println("[Ingestion] Trimming seen-IDs cache...");
                    seenIds.clear();
                }

                Thread.sleep(POLL_INTERVAL * 1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Ingestion] Interrupted. Exiting...");
        }
    }

    // ── GitHub API ────────────────────────────────────────────────────────────
    private static List<JsonNode> fetchEvents() {
        List<JsonNode> result = new ArrayList<>();
        HttpURLConnection conn = null;
        try {
            URL url = new URL(GITHUB_EVENTS_URL);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(15_000);
            conn.setReadTimeout(15_000);
            conn.setRequestProperty("Accept", "application/vnd.github+json");
            conn.setRequestProperty("X-GitHub-Api-Version", "2022-11-28");
            conn.setRequestProperty("User-Agent", "CS523-BDT-Final/1.0");
            if (!GITHUB_TOKEN.isEmpty())
                conn.setRequestProperty("Authorization", "Bearer " + GITHUB_TOKEN);

            int    status    = conn.getResponseCode();
            String remaining = conn.getHeaderField("X-RateLimit-Remaining");

            if (status == 200) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) sb.append(line);
                    JsonNode arr = MAPPER.readTree(sb.toString());
                    if (arr.isArray()) arr.forEach(result::add);
                }
                System.out.printf("[Ingestion] Fetched %d events — rate-limit remaining: %s%n",
                    result.size(), remaining != null ? remaining : "?");

            } else if (status == 403) {
                System.out.printf("[Ingestion] Rate limited (403). Remaining: %s. Sleeping 60s...%n", remaining);
                Thread.sleep(60_000);

            } else if (status == 304) {
                System.out.println("[Ingestion] 304 Not Modified — no new events.");

            } else {
                System.err.println("[Ingestion] GitHub API error: HTTP " + status);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.err.println("[Ingestion] Network error: " + e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
        return result;
    }

    // ── Event enrichment ──────────────────────────────────────────────────────
    private static ObjectNode enrichEvent(JsonNode raw) {
        String    eventType = raw.path("type").asText("Unknown");
        JsonNode  actor     = raw.path("actor");
        JsonNode  repo      = raw.path("repo");
        JsonNode  payload   = raw.path("payload");

        ObjectNode out = MAPPER.createObjectNode();

        // Core fields
        out.put("event_id",     raw.path("id").asText(""));
        out.put("event_type",   eventType);
        out.put("actor_login",  actor.path("login").asText(""));
        out.put("actor_id",     actor.path("id").asLong(0));
        out.put("repo_id",      repo.path("id").asLong(0));
        out.put("repo_name",    repo.path("name").asText(""));
        out.put("created_at",   raw.path("created_at").asText(""));
        out.put("ingest_time",  Instant.now().toString());
        out.put("trend_weight", EVENT_WEIGHTS.getOrDefault(eventType, 0));

        // Type-specific fields (defaults)
        out.put("push_size",    0);
        out.put("push_ref",     "");
        out.put("ref_type",     "");
        out.put("fork_repo",    "");
        out.put("release_tag",  "");
        out.put("pr_action",    "");
        out.put("issue_action", "");
        out.put("language",     "");

        switch (eventType) {
            case "PushEvent":
                out.put("push_size", payload.path("size").asInt(0));
                out.put("push_ref",  payload.path("ref").asText(""));
                break;
            case "CreateEvent":
                out.put("ref_type", payload.path("ref_type").asText(""));
                break;
            case "ForkEvent":
                out.put("fork_repo", payload.path("forkee").path("full_name").asText(""));
                break;
            case "ReleaseEvent":
                out.put("release_tag", payload.path("release").path("tag_name").asText(""));
                break;
            case "PullRequestEvent":
                out.put("pr_action", payload.path("action").asText(""));
                break;
            case "IssuesEvent":
                out.put("issue_action", payload.path("action").asText(""));
                break;
            default:
                break;
        }
        return out;
    }

    // ── Kafka producer factory ────────────────────────────────────────────────
    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,       "all");
        props.put(ProducerConfig.RETRIES_CONFIG,    3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,  100);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);

        // Optional MSK SASL_SSL / SCRAM
        if ("SASL_SSL".equalsIgnoreCase(SECURITY_PROTOCOL)) {
            System.out.println("[Ingestion] SASL_SSL enabled — mechanism: " + SASL_MECHANISM);
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", SASL_MECHANISM);
            props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"" + SASL_USERNAME + "\" " +
                "password=\"" + SASL_PASSWORD + "\";");
        }

        System.out.println("[Ingestion] Connecting to Kafka: " + KAFKA_BOOTSTRAP);
        return new KafkaProducer<>(props);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static String cfg(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : def;
    }

    private static void printBanner() {
        System.out.println("=================================================");
        System.out.println("  BDT Final — GitHub Activity Ingestion");
        System.out.println("  Kafka:    " + KAFKA_BOOTSTRAP + "  →  " + KAFKA_TOPIC);
        System.out.println("  Interval: " + POLL_INTERVAL + "s");
        System.out.println("  Token:    " + (GITHUB_TOKEN.isEmpty() ? "NO (60 req/hr)" : "YES (5000 req/hr)"));
        System.out.println("  SASL:     " + (SECURITY_PROTOCOL.isEmpty() ? "off (PLAINTEXT)" : SECURITY_PROTOCOL));
        System.out.println("=================================================");
    }
}

