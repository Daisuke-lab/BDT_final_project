# BigData2026 — Final Project

## Requirements
- JDK 11+
- Docker

## Run locally (sbt)

```bash
./sbt ingestion/run
./sbt streaming/run
./sbt vizBackend/run
./sbt vizFrontend/fastLinkJS
```

## Build & deploy (Docker)

```bash
# Build all images
bash infra/build-all.sh

# Spin up the full cluster
bash infra/up.sh

# Or build one service at a time
bash infra/ingestion/build.sh
bash infra/streaming/build.sh
bash infra/visualization/backend/build.sh
bash infra/visualization/frontend/build.sh
```
