# Real-Time GitHub Open Source Trend Analytics Dashboard

## Goal

Build a live dashboard that surfaces current trends in public GitHub activity. Public GitHub events are collected, streamed through Kafka, processed by Spark Structured Streaming, stored in Hive, and visualized in a real-time dashboard.

The goal is not to display raw GitHub data, but to turn public developer activity into meaningful insights about open-source trends.

---

## Architecture

```
GitHub API → Kafka → Spark Structured Streaming → Hive → Dashboard
                                                       ↑
                                          Static language metadata (Spark SQL join)
```

| Layer | Technology | Role |
|-------|-----------|------|
| Ingestion | Kafka | Collects and buffers public GitHub events |
| Processing | Spark Structured Streaming | Real-time aggregations and trend scoring |
| Storage | Hive | Persists aggregated results for the dashboard |
| Visualization | Dashboard (backend + frontend) | Displays live insights |
| Bonus | Spark SQL join | Enriches events with static language/ecosystem metadata |

---

## Dashboard Features

### New repositories created over time
Count of new public repositories created every 5 minutes. Shows the pace of open-source project creation.

### Source-code activity speed
Tracks `PushEvent` volume to estimate how fast code is being added or updated across all repositories.

### Top active repositories
Ranks repositories by recent activity across event types: pushes, pull requests, issues, stars, forks, and releases.

### Trending repositories
Computes a weighted trend score per repository:

```
score = stars + forks + pushes + pull_requests + issues + releases
```

Repositories with the fastest-growing scores surface as trending.

### Trending programming languages
Joins live GitHub activity with a static language metadata dataset (Spark SQL bonus) to show which languages are currently most active.

### Stars and forks momentum
Tracks which repositories are gaining attention quickly by measuring the rate of `WatchEvent` and `ForkEvent` over sliding time windows.

### Release activity
Highlights repositories publishing new releases in real time.

### Event type distribution
Shows the percentage breakdown of all GitHub activity by event type: push, star, fork, issue, pull request, release.

---

## Requirements Coverage

| Requirement | How it is met |
|-------------|---------------|
| Kafka ingestion | GitHub API events collected and published to a Kafka topic |
| Spark Structured Streaming | Windowed aggregations, trend scoring, momentum tracking |
| Persistent storage | Aggregated results written to Hive |
| Visualization | Live dashboard reads from Hive via the backend API |
| Bonus — Spark SQL join | Live events joined with static language metadata for language trend analysis |
