# AutoMeta — Automated Metadata for Data Engineering Workflows

> A research project demonstrating fully automated schema change detection, downstream impact analysis, and orchestration-driven self-healing for metadata-driven data pipelines.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-E25A1C?logo=apache-spark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-00ADD8)](https://delta.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://postgresql.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-6-47A248?logo=mongodb)](https://mongodb.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docker.com)

---

## Overview

Modern data engineering teams spend **30–40% of their time** on pipeline maintenance triggered by upstream schema changes — a new column, a renamed field, a type regression. These changes are rarely detected automatically and almost never traced to their downstream impact before breakage occurs.

**AutoMeta** solves this by building a metadata-driven layer on top of Apache Airflow and Spark:

| Problem | AutoMeta Solution |
|---------|-----------------|
| Schema changes go undetected | Schema Registry automatically versions and diffs every source |
| Unknown downstream impact | Lineage Graph (NetworkX DAG) traverses all downstream dependencies |
| Manual triage and fixes | Orchestration layer auto-resolves LOW risk, flags MEDIUM, pauses HIGH |
| Slow pipeline onboarding | DAGs generated dynamically from config — no code changes needed |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Airflow (Orchestration Layer)                       │
│          dag_factory.py — dynamically generates DAGs from metadata       │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
           ┌───────────────▼───────────────┐
           │    PipelineEngine (Spark)      │
           │  spark_jobs/pipeline_engine.py │
           └───────────────┬───────────────┘
                           │
     ┌─────────────────────▼───────────────────────┐
     │               Metadata Manager               │
     │        src/metadata_manager/manager.py        │
     │   PostgreSQL • MongoDB • File-based (dev)     │
     └──────────┬──────────────────────┬────────────┘
                │                      │
   ┌────────────▼─────────┐  ┌─────────▼──────────────┐
   │   Schema Registry    │  │    Impact Analyzer       │
   │  schema_registry/    │  │  impact_analysis/        │
   │  registry.py         │  │  lineage_graph.py        │
   │  • register schema   │─▶│  • NetworkX DiGraph      │
   │  • detect drift      │  │  • BFS traversal         │
   │  • log changes       │  │  • risk scoring          │
   └──────────────────────┘  └────────────────────────┘
                │
   ┌────────────▼──────────────┐
   │  Change Detection Engine   │
   │  change_detection/         │
   │  detector.py               │
   │  • null checks             │
   │  • type consistency        │
   └───────────────────────────┘
```

**Risk Score Formula:** `risk = num_downstream_tables × max_depth`

| Severity | Threshold | Orchestration Action |
|----------|-----------|---------------------|
| 🟢 LOW | `risk ≤ 2` | AUTO-UPDATE metadata, continue pipeline |
| 🟡 MEDIUM | `2 < risk ≤ 5` | FLAG for manual review, log warning |
| 🔴 HIGH | `risk > 5` | PAUSE pipeline, send alert |

---

## Project Structure

```
AutoMeta/
├── config/                         # Pipeline & system configuration
│   ├── metadata_config.yaml        # MetadataManager config
│   └── orders_pipeline.yaml        # Example pipeline definition
│
├── dags/                           # Airflow DAG layer
│   └── dag_factory.py              # Dynamic DAG generator (metadata-driven)
│
├── datasets/                       # Source datasets
│   └── orders.csv                  # Sample TPC-H orders data
│
├── spark_jobs/                     # Spark execution layer
│   └── pipeline_engine.py          # Config-driven transformation engine
│
├── src/                            # Core Python package
│   └── metadata_manager/
│       └── manager.py              # Central metadata hub (schemas, lineage, logs)
│
├── schema_registry/                # Schema versioning & drift detection
│   └── registry.py                 # register + detect_changes + orchestration hook
│
├── impact_analysis/                # Downstream impact evaluation
│   └── lineage_graph.py            # NetworkX lineage graph + risk scoring
│
├── change_detection/               # Data quality validation
│   └── detector.py                 # Null checks + type consistency rules
│
├── init-db/                        # PostgreSQL DDL
│   └── init.sql                    # Tables: source_schemas, lineage, schema_change_log
│
├── tests/                          # Unit tests
│
├── logs/                           # Runtime outputs (gitignored)
│   ├── schemas/                    # Registered schema snapshots
│   ├── schema_change_log.json      # Change event log
│   └── evaluation_results.json     # Phase 7 evaluation results
│
├── demo_script.py                  # Runs all 5 evaluation scenarios end-to-end
├── simulate_schema_drift.py        # Phase 5/6 drift + orchestration demo
├── Dockerfile.airflow              # Custom Airflow image (Java 11 + PySpark + Delta)
├── docker-compose.yml              # Full stack: Airflow + Spark + Postgres + MongoDB
├── .env.example                    # Environment variable template
└── README.md
```

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose v2+
- Python 3.10+ (for running the demo script locally)

### 1. Clone & Configure

```bash
git clone https://github.com/<your-username>/autometa.git
cd autometa

# Copy the environment template and fill in your values
cp .env.example .env
```

> **Note:** The default `.env.example` values work out-of-the-box for local development. Change passwords before any public deployment.

### 2. Start the Full Stack

```bash
docker compose up --build -d
```

This starts:

| Service | URL | Description |
|---------|-----|-------------|
| Airflow Webserver | http://localhost:8080 | DAG UI — login `admin / admin` |
| Spark Master | http://localhost:8081 | Spark cluster UI |
| PostgreSQL | `localhost:5432` | Metadata + Airflow DB |
| MongoDB | `localhost:27017` | Document store |

### 3. Run the Evaluation Demo (no Docker required)

```bash
pip install networkx pyyaml
python3 demo_script.py
```

Expected output: all 5 evaluation scenarios with a formatted results table.

---

## Evaluation Scenarios (Phase 7)

Run `python3 demo_script.py` to reproduce all results. Results are saved to `logs/evaluation_results.json`.

| # | Scenario | Detection Latency | Pipelines Affected | Severity | Action |
|---|----------|------------------|--------------------|----------|--------|
| 1 | Add a column | ~2 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 2 | Rename a field | ~0.5 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 3 | Change a data type | ~0.5 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 4 | Introduce missing values | ~0 ms | 1 | 🟡 MEDIUM | FLAG |
| 5 | Add a new data source | ~0.1 ms | 0 | 🟢 LOW | AUTO-UPDATE |

**Key result:** Automated detection and impact analysis completes in **< 3 ms** vs **30+ minutes** manually.

---

## Configuration

### Pipeline Configuration (`config/<pipeline_name>.yaml`)

```yaml
source:
  path: "/opt/airflow/datasets/orders.csv"
target:
  path: "/opt/airflow/datasets/clean_orders"
transformations:
  - type: rename_column
    from: "o_orderkey"
    to: "order_id"
  - type: cast
    column: "o_totalprice"
    to: "double"
  - type: drop_nulls
    columns: ["o_totalprice"]
validation_rules:
  - "totalprice > 0"
```

Adding a new pipeline requires only a new YAML file — no DAG code changes.

### Environment Variables (`.env`)

| Variable | Description |
|----------|-------------|
| `AIRFLOW_FERNET_KEY` | Airflow encryption key (generate with `cryptography.fernet`) |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Airflow webserver session key |
| `POSTGRES_USER / PASSWORD / DB` | PostgreSQL credentials |
| `MONGO_ROOT_USERNAME / PASSWORD` | MongoDB credentials |
| `SPARK_MASTER_URL` | Spark master connection URL |

---

## Troubleshooting

### Airflow Init Fails
```bash
docker logs metadata-airflow-init-1
```
Common causes:
- **openlineage version conflict** → Already fixed by pinning `apache-airflow-providers-openlineage>=1.8.0` in `Dockerfile.airflow`
- **DB not ready** → Postgres healthcheck retries 10× with 5s delay; increase if needed

### Docker Permission Denied on `compose down`
If containers were started as root:
```bash
sudo docker rm -f $(sudo docker ps -aq)
docker compose up -d
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.10 |
| Processing | Apache Spark 3.4.1 |
| Storage Format | Delta Lake 2.4 |
| Relational Store | PostgreSQL 15 |
| Document Store | MongoDB 6 |
| Graph Library | NetworkX |
| Containerisation | Docker Compose |
| Language | Python 3.10+ |

---

## License

MIT License — see [LICENSE](LICENSE) for details.
