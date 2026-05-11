package com.bigdata2026.schema;

/**
 * Single source of truth for the GitHub event Kafka contract.
 *
 * Each Kafka message has two layers:
 *
 *   Envelope (outer JSON):
 *     { "id": "...", "source": "github", "payload": "<escaped JSON>", "timestamp": <epoch_ms> }
 *
 *   Payload (inner JSON, value of the "payload" field):
 *     { "event_type": "PushEvent", "actor_login": "alice", "repo_name": "org/alpha",
 *       "trend_weight": 2, "push_size": 3, ... }
 *
 * Both ingestion and streaming import this class so field names cannot drift.
 */
public final class GitHubEventSchema {
    private GitHubEventSchema() {}

    /** Kafka topic shared by producer and consumers. */
    public static final String TOPIC = "github-events";

    // ── Envelope fields (outer JSON) ──────────────────────────────────────────

    /** Unique GitHub event ID — also used as Kafka message key. */
    public static final String E_ID        = "id";
    /** Always "github". */
    public static final String E_SOURCE    = "source";
    /** Enriched event as an escaped JSON string. Parse with inner payload schema. */
    public static final String E_PAYLOAD   = "payload";
    /** Ingestion epoch-millisecond timestamp (when the producer received the event). */
    public static final String E_TIMESTAMP = "timestamp";

    // ── Payload fields (inner JSON, inside E_PAYLOAD) ─────────────────────────

    /** GitHub event ID (same as E_ID, duplicated inside payload for self-containment). */
    public static final String P_EVENT_ID     = "event_id";
    /** GitHub event type, e.g. "PushEvent", "WatchEvent", "ForkEvent". */
    public static final String P_EVENT_TYPE   = "event_type";

    /** GitHub login of the actor who triggered the event. */
    public static final String P_ACTOR_LOGIN  = "actor_login";
    /** Numeric GitHub user ID of the actor. */
    public static final String P_ACTOR_ID     = "actor_id";

    /** Numeric GitHub repository ID. */
    public static final String P_REPO_ID      = "repo_id";
    /** Full repository name, e.g. "owner/repo". */
    public static final String P_REPO_NAME    = "repo_name";

    /** ISO-8601 event creation time as reported by GitHub. */
    public static final String P_CREATED_AT   = "created_at";
    /** ISO-8601 timestamp when the ingestion service processed this event. */
    public static final String P_INGEST_TIME  = "ingest_time";

    /**
     * Trend weight assigned by ingestion based on event type:
     *   WatchEvent (star)=3, PushEvent=2, ForkEvent=2, ReleaseEvent=2,
     *   IssuesEvent=1, PullRequestEvent=1, CreateEvent=1, others=0.
     */
    public static final String P_TREND_WEIGHT = "trend_weight";

    // ── Type-specific payload fields (null/empty when not applicable) ─────────

    /** PushEvent: number of commits in the push. */
    public static final String P_PUSH_SIZE    = "push_size";
    /** PushEvent: git ref, e.g. "refs/heads/main". */
    public static final String P_PUSH_REF     = "push_ref";

    /** CreateEvent: what was created — "branch", "tag", or "repository". */
    public static final String P_REF_TYPE     = "ref_type";

    /** ForkEvent: full name of the forked repository. */
    public static final String P_FORK_REPO    = "fork_repo";

    /** ReleaseEvent: tag name of the release, e.g. "v1.2.0". */
    public static final String P_RELEASE_TAG  = "release_tag";

    /** PullRequestEvent: action — "opened", "closed", "merged", etc. */
    public static final String P_PR_ACTION    = "pr_action";

    /** IssuesEvent: action — "opened", "closed", "labeled", etc. */
    public static final String P_ISSUE_ACTION = "issue_action";

    /** Reserved for Spark SQL join with static language metadata (bonus feature). */
    public static final String P_LANGUAGE     = "language";
}
