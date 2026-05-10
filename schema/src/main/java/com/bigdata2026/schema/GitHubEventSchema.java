package com.bigdata2026.schema;

/**
 * Single source of truth for the GitHub event Kafka contract.
 * Both the producer (ingestion) and consumer (streaming) depend on this class,
 * so field name drift is caught at compile time rather than silently at runtime.
 */
public final class GitHubEventSchema {
    private GitHubEventSchema() {}

    /** Kafka topic both modules agree on. */
    public static final String TOPIC = "github-events";

    /** Top-level JSON field: event type (e.g. "PushEvent"). */
    public static final String F_TYPE       = "type";

    /** Top-level JSON field: nested actor object. */
    public static final String F_ACTOR      = "actor";
    /** Nested inside actor: GitHub login. */
    public static final String F_LOGIN      = "login";

    /** Top-level JSON field: nested repo object. */
    public static final String F_REPO       = "repo";
    /** Nested inside repo: full repo name (e.g. "org/alpha"). */
    public static final String F_REPO_NAME  = "name";

    /** Top-level JSON field: ISO-8601 event timestamp string. */
    public static final String F_CREATED_AT = "created_at";
}
