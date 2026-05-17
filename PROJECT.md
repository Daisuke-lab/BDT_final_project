# Real-Time GitHub Open Source Trend Analytics

## Goal

Build a live dashboard that surfaces current trends in public GitHub activity. GitHub public events are ingested in real time via Kafka, processed by two parallel streaming pipelines (ZIO Kafka and Apache Spark Structured Streaming), stored in HBase, and visualized in a WebSocket-driven dashboard. A bonus Spark SQL enrichment joins streaming data with static reference datasets stored in HDFS.

---

## Architecture

```
GitHub Public Events API
         │
         ▼ (Java 11 producer, polls every 10s)
  Kafka Topic: github-events
         │
         ├─────────────────────────────────────────────────────┐
         ▼                                                     ▼
ZIO Kafka Consumer                            Spark Structured Streaming
(5-min tumbling windows)                      (5s micro-batches + Spark SQL)
         │                                                     │
         ▼                                           ┌─────────┴──────────┐
HBase Tables:                                        ▼                    ▼
  new_repos_per_window                        HBase Table:          HBase Table:
  push_speed_per_window                         repo_stats            language_stats
  (window counts for                            (stars, forks,        (Spark SQL join
   new repo creation                            pushes, contribs,     with HDFS CSVs:
   and push velocity)                           activity per repo)    lang tier, paradigm)
         │                                           │                    ▲
         │                                           │             HDFS Static Data:
         │                                           │               language_rankings.csv
         └────────────────────┬──────────────────────┘
                              ▼
              ZIO-Http Backend — WebSocket /api/subscribe
                              │
                              ▼
           Tyrian / Scala.js Frontend Dashboard
              (live panels, auto-reconnects)
```

| Layer | Technology | Role |
|-------|-----------|------|
| Ingestion | Java 11 + Kafka Producer | Polls GitHub API, publishes enriched events |
| Message Queue | Apache Kafka | Buffers and distributes events |
| Real-time Streaming | ZIO + zio-kafka | Windowed aggregations (new repos, push speed) |
| Batch Streaming | Apache Spark Structured Streaming | Repo-level aggregations, Spark SQL enrichment |
| Static Storage | HDFS | Hosts `language_rankings.csv` (language metadata) |
| Persistent Storage | Apache HBase | Real-time key-value store for all aggregated results |
| Backend API | ZIO-Http + WebSocket | Reads HBase, broadcasts live updates to clients |
| Frontend | Tyrian (Elm-style) + Scala.js | Live dashboard, 8 panels |
| Styling | Tailwind CSS 4 + DaisyUI 5 | Component styling |
| Infrastructure | Docker Compose | Local dev and full-cluster deployment |

---

## Requirements Coverage

| Part | Requirement | Points | How It Is Met |
|------|-------------|--------|---------------|
| **1** | Real-Time Data Ingestion (Apache Kafka) | 3 | Java 11 producer polls GitHub public Events API every 10s. Enriches each event with trend weight, type-specific fields (push size, fork repo, release tag, PR/issue action, language). Publishes to `github-events` Kafka topic. Supports SASL_SSL for AWS MSK. |
| **2** | Distributed Processing (Spark Structured Streaming) | 3 | `RepoStatsJob.scala` subscribes to the Kafka topic, aggregates repo-level metrics (stars, forks, pushes, push weight, activity count, unique contributors) per 5-second micro-batch using `groupBy` + windowed aggregations. Also performs a Spark SQL join with HDFS reference data (see Part 5). |
| **3** | Persistent Storage (HBase) | 2 | Three HBase tables: `repo_stats` (Spark, per-repo metrics), `new_repos_per_window` (ZIO, 5-min window counts), `push_speed_per_window` (ZIO, 5-min push counts), `language_stats` (Spark SQL enriched language metrics). HBase enables rapid real-time key-value lookups for the dashboard. |
| **4** | Visualization & Dashboarding | 2 | ZIO-Http WebSocket backend broadcasts live HBase data. Tyrian / Scala.js frontend renders 8 dashboard panels. Panels update every 5–60 seconds via WebSocket pushes. |
| **5 (bonus)** | Data Enrichment with Spark SQL | 2 | Spark SQL joins live `repo_stats` with `language_rankings.csv` from HDFS (language tier, paradigm, type). Language comes directly from streaming events. Enriched language-level aggregates are written to HBase `language_stats` and visualized in the "Top Languages" dashboard panel. |

---

## Dashboard Panels

| # | Panel | Data Source | Update Interval |
|---|-------|------------|-----------------|
| 1 | Overview Stats (repos, stars, forks, pushes, contributors) | HBase `repo_stats` | 5s |
| 2 | Top Repos by New Stars | HBase `repo_stats` | 5s |
| 3 | Top Repos by New Forks | HBase `repo_stats` | 5s |
| 4 | Top Repos by Activity (total events) | HBase `repo_stats` | 5s |
| 5 | Top Repos by Contributors | HBase `repo_stats` | 5s |
| 6 | New Repos per 5-min Window | HBase `new_repos_per_window` | 30s |
| 7 | Push Speed per 5-min Window | HBase `push_speed_per_window` | 30s |
| 8 | Top Languages by Activity (Spark SQL enriched) | HBase `language_stats` | 60s |

---

## HBase Tables

| Table | Writer | Row Key | Columns |
|-------|--------|---------|---------|
| `repo_stats` | Spark Structured Streaming | repo name | star_count, fork_count, push_count, push_weight, activity_count, contributor_count |
| `new_repos_per_window` | ZIO Kafka Consumer | window start timestamp (hex) | count, window_start, window_end |
| `push_speed_per_window` | ZIO Kafka Consumer | window start timestamp (hex) | count, window_start, window_end |
| `language_stats` | Spark SQL enrichment | language name | tier, paradigm, lang_type, total_stars, total_forks, total_pushes, total_activity, repo_count |

---

## HDFS Static Reference Data (Part 5)

| File | Contents | Used For |
|------|----------|---------|
| `/data/language_rankings.csv` | 50 programming languages → tier (1/2/3), paradigm, type | Enriches event language with metadata for aggregation |

The Spark SQL enrichment:
1. Join `repo_stats_batch` with `language_rankings` on `event_language` → adds tier, paradigm, type
2. Aggregate by language → `language_stats` written to HBase

---

## Data Flow: Kafka Event Envelope

```json
{
  "id":        "github_event_id",
  "source":    "github",
  "payload":   "{\"event_type\":\"PushEvent\",\"repo_name\":\"owner/repo\",\"language\":\"Go\",...}",
  "timestamp": 1715636400000
}
```

Payload fields: `event_type`, `actor_login`, `actor_id`, `repo_id`, `repo_name`, `created_at`, `ingest_time`, `trend_weight`, `push_size`, `push_ref`, `ref_type`, `fork_repo`, `release_tag`, `pr_action`, `issue_action`, `language`

---

## Running the Project

### Local development (sbt)

```bash
# 1. Start infrastructure
bash infra/dev.sh           # Kafka + HBase + HDFS

# 2. Run each service
sbt ingestion/run           # Part 1: GitHub → Kafka
sbt streaming/run           # Part 2/3: ZIO Kafka → HBase windows
sbt sparkStreaming/run      # Part 2/3/5: Spark → HBase repo_stats + language_stats
sbt vizBackend/run          # Part 4: WebSocket backend (port 8080)
sbt vizFrontend/dev         # Part 4: Vite frontend (http://localhost:9876)
```

### Docker (full cluster)

```bash
bash infra/build-all.sh     # Build all images
bash infra/up.sh            # Start full cluster
# Dashboard at http://localhost:3000
# HDFS UI  at http://localhost:9870
# HBase UI at http://localhost:16010
bash infra/down.sh          # Stop cluster
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | — | GitHub PAT (5000 req/hr vs 60 unauthenticated) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `HBASE_ZOOKEEPER_QUORUM` | `localhost` | HBase ZooKeeper host |
| `HDFS_NAMENODE_URL` | `hdfs://namenode:9000` | HDFS NameNode URI |
| `KAFKA_SECURITY_PROTOCOL` | — | Set to `SASL_SSL` for AWS MSK |
