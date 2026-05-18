```
  ██████╗ ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗
  ██╔══██╗██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗
  ██████╔╝██║██║  ███╗    ██║  ██║███████║   ██║   ███████║
  ██╔══██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║
  ██████╔╝██║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║
  ╚═════╝ ╚═╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝

  GitHub Pulse — Real-time Repository Analytics
  Kafka · Spark Streaming · HBase
```
<img width="1278" height="972" alt="image" src="https://github.com/user-attachments/assets/ab9a48fc-1afb-41fa-8b90-d8b08001c64a" />

<img width="1276" height="499" alt="image" src="https://github.com/user-attachments/assets/5a6d4992-5bc3-4efc-a5f2-edff1a4d13db" />

## Data Flow

```mermaid
flowchart LR
    GH(["GitHub API"])

    subgraph Ingestion["Ingestion — Daisuke"]
        ING["ingestion\n(Java)"]
    end

    subgraph Messaging["Messaging — Daisuke"]
        ZK["Zookeeper"]
        KF["Kafka\ntopic: github-events"]
        ZK --> KF
    end

    subgraph Processing["Processing — shared"]
        SS["Spark Streaming\n(Scala)"]
    end

    subgraph Storage["Storage — shared"]
        HB[("HBase")]
        NN[("HDFS\nNameNode")]
        DN[("HDFS\nDataNode")]
        NN --- DN
    end

    subgraph Visualization["Visualization — Bao Khanh Dinh"]
        BE["Backend\n(zio-http :8080)"]
        FE["Frontend\n(:3000)"]
        BE -->|"WebSocket"| FE
    end

    GH -->|"poll every 10s"| ING
    ING -->|"produce"| KF
    KF -->|"consume"| SS
    SS -->|"write analytics"| HB
    SS -->|"write raw events"| NN
    HB -->|"read"| BE
```

## Requirements

- JDK 11+
- Docker
- GitHub personal access token

## Run locally

```bash
GITHUB_TOKEN=<your_token> bash infra/dev.sh
```
## Live
https://visualization-frontend.lifespacedigital.com/
