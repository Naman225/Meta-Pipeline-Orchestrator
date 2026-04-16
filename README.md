# Meta-Pipeline-Orchestrator

> **Automated Metadata-Driven Data Engineering Pipeline**  
> Schema drift detection · Lineage-based impact analysis · **Autonomous Schema Evolution** · **Interactive Research Dashboard**

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-00ADD8)](https://delta.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-6-47A248?logo=mongodb&logoColor=white)](https://mongodb.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![Tests](https://img.shields.io/badge/Tests-38%20passed-brightgreen)](./tests/)

---

## The Problem

In every real-world data engineering team, someone upstream changes a database column — renames it, changes its type, or adds a new field. Nobody tells the downstream team. The pipeline silently breaks. Hours are wasted debugging. Dashboards show wrong numbers.

This project builds a system that **catches those changes automatically**, figures out every downstream table or pipeline that would be affected, and takes the right action — all without human intervention.

---

## What This System Does

```
Data Source changes schema
        ↓
System detects the drift automatically (< 3ms)
        ↓
Builds a lineage graph of ALL downstream dependencies
        ↓
Calculates a risk score based on depth + breadth of impact
        ↓
Makes an automated decision:
  LOW risk   → **Autonomous Adjustment**: Auto-update metadata, continue workflow
  MEDIUM risk → **Flagged Evolution**: Raise warning, user review while proceeding
  HIGH risk  → **Circuit Break**: Pause pipeline, trigger alert to prevent data corruption
```

---

## 🚀 New: Interactive Research Dashboard

To visualize the system's decision-making in real-time, we provide a sleek, dark-mode dashboard. This allows you to simulate and observe the pipeline's autonomous behavior without digging into raw logs.

**Features:**
- **Visual Schema Diffs**: Real-time highlighting of added (Green), removed (Red), or modified (Yellow) columns.
- **Custom Dataset Analytics**: Upload any CSV/JSON file to test how the system onboard new sources and detects drift autonomously.
- **Latency & Risk Monitoring**: Live dials showing detection performance (< 5ms) and risk scores.

### Running the Dashboard
```bash
# Install additional dependencies
pip install fastapi uvicorn pandas python-multipart

# Start the dashboard server
python3 api.py
```
View at: **http://localhost:8000**

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION LAYER                   │
│              dag_factory.py — metadata-driven DAGs               │
└──────────────────────────┬──────────────────────────────────────┘
                           │  submits jobs to
          ┌────────────────▼─────────────────┐
          │         SPARK CLUSTER             │
          │     pipeline_engine.py            │
          │  runs config-driven transforms    │
          └────────────────┬─────────────────┘
                           │  reads/writes via
     ┌─────────────────────▼──────────────────────┐
     │              METADATA MANAGER               │
     │           src/metadata_manager/             │
     │  • loads pipeline configs from YAML         │
     │  • stores schema versions (file / Postgres) │
     │  • writes append-only change audit log      │
     │  • provides lineage edges to analyzer       │
     └────────────┬──────────────────┬────────────┘
                  │                  │
   ┌──────────────▼────┐   ┌─────────▼──────────────┐
   │  SCHEMA REGISTRY  │   │    IMPACT ANALYZER      │
   │  registry.py      │   │    lineage_graph.py     │
   │                   │──▶│                         │
   │  • infer schema   │   │  • builds NetworkX DAG  │
   │  • version it     │   │  • BFS traversal        │
   │  • diff old/new   │   │  • computes risk score  │
   │  • log changes    │   │  • returns severity     │
   └──────────┬────────┘   └─────────────────────────┘
              │
   ┌──────────▼───────────────┐
   │  VALIDATION DETECTOR     │
   │  change_detection/       │
   │  detector.py             │
   │  • NOT_NULL checks       │
   │  • TYPE_CONSISTENCY      │
   └──────────────────────────┘
```

---

## Risk Scoring Engine

Every schema change is scored using a simple but powerful formula:

```
Risk Score = Number of Impacted Downstream Tables × Maximum Lineage Depth
```

| Score | Severity | Automated Action |
|-------|----------|-----------------|
| ≤ 2 | 🟢 LOW | Auto-update metadata → pipeline continues normally |
| 3–5 | 🟡 MEDIUM | Flag for review → pipeline continues cautiously |
| > 5 | 🔴 HIGH | Pause pipeline → alert raised immediately |

**Example:** If changing `total_price` impacts `clean_orders → feature_orders → ml_input → dashboard` (4 tables, depth 4), the risk score is **16 → HIGH → PAUSE & ALERT**.

---

## Project Structure

```
Meta-Pipeline-Orchestrator/
│
├── src/
│   └── metadata_manager/
│       └── manager.py              ← Central hub: schema storage, lineage, logs
│
├── schema_registry/
│   └── registry.py                 ← Drift detector + orchestration decision hook
│
├── impact_analysis/
│   └── lineage_graph.py            ← NetworkX lineage graph + BFS risk scoring
│
├── change_detection/
│   └── detector.py                 ← Null & type-consistency validation rules
│
├── spark_jobs/
│   ├── pipeline_engine.py          ← Config-driven Spark transform executor
│   └── test_run.py                 ← Spark integration test (requires Spark)
│
├── dags/
│   └── dag_factory.py              ← Airflow: auto-generates DAGs from metadata
│
├── config/
│   ├── metadata_config.yaml        ← Global system config
│   └── orders_pipeline.yaml        ← Example pipeline: source, transforms, target
│
├── datasets/
│   └── orders.csv                  ← Sample TPC-H–style orders data
│
├── init-db/
│   └── init.sql                    ← PostgreSQL DDL for production metadata store
│
├── tests/                          ← 38 unit tests (no Spark/Docker needed)
│   ├── test_schema_registry.py     ← 13 tests: register, drift detection, log
│   ├── test_impact_analyzer.py     ← 12 tests: lineage graph, BFS, risk score
│   └── test_validation_detector.py ← 13 tests: nulls, types, combined rules
│
├── metadata_logs/
│   └── .gitkeep                    ← Runtime log dir (schemas + change_log)
│
├── api.py                          ← FastAPI backend for Research Dashboard
├── dashboard/                      ← Web UI assets (HTML, CSS, JS)
├── demo_script.py                  ← Run all 5 evaluation scenarios end-to-end
├── simulate_schema_drift.py        ← Targeted drift + orchestration demo
├── docker-compose.yml              ← Full stack: Airflow + Spark + Postgres + Mongo
├── Dockerfile.airflow              ← Custom Airflow image (Java 11 + PySpark + Delta)
├── .env.example                    ← Template for environment secrets
└── README.md
```

---

## Quick Start

### Option A — Run the Demo (No Docker, No Spark)

This runs all 5 evaluation scenarios and prints the results table. Only needs `networkx` and `pyyaml`.

```bash
git clone https://github.com/Naman225/Meta-Pipeline-Orchestrator.git
cd Meta-Pipeline-Orchestrator

# Install minimal dependencies
pip install networkx pyyaml

# Run the full evaluation suite
python3 demo_script.py

# Run the focused drift + orchestration demo
python3 simulate_schema_drift.py
```

### Option B — Run the Unit Tests

```bash
pip install pytest
python3 -m pytest tests/ -v
# Expected: 38 passed
```

### Option C — Full Stack with Docker

```bash
# 1. Set up environment
cp .env.example .env

# 2. Build and start all services
docker compose up --build -d

# 3. Wait ~60 seconds for Airflow to initialize, then open:
#    Airflow UI  → http://localhost:8085  (admin / admin)
#    Spark UI    → http://localhost:8086

# 4. To stop everything
docker compose down
```

---

## Services & Ports

| Service | URL / Port | Credentials |
|---------|-----------|-------------|
| Airflow Webserver | http://localhost:8085 | `admin` / `admin` |
| Spark Master UI | http://localhost:8086 | — |
| Spark RPC | `localhost:7080` | — (internal: 7077) |
| PostgreSQL | `localhost:5433` | See `.env` |
| MongoDB | `localhost:27019` | See `.env` |

---

## Evaluation Results

Run `python3 demo_script.py` to reproduce all results locally.

| # | Scenario | Latency | Pipelines Affected | Severity | Action |
|---|----------|:-------:|:-----------------:|----------|--------|
| 1 | Add a column | 2.2 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 2 | Rename a field | 0.4 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 3 | Change a data type | 0.3 ms | 4 | 🔴 HIGH | PAUSE & ALERT |
| 4 | Introduce missing values | < 1 ms | 1 | 🟡 MEDIUM | FLAG |
| 5 | Onboard a new data source | 0.1 ms | 0 | 🟢 LOW | AUTO-UPDATE |

**Key finding:** Automated detection + impact analysis completes in **< 3 ms** compared to a manual baseline of **~30 minutes** per source.

---

## How a Pipeline is Defined

The system is **metadata-driven** — adding a new pipeline requires only a YAML file. No Python or DAG code changes needed.

```yaml
# config/orders_pipeline.yaml
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

`dag_factory.py` reads all active pipeline configs and auto-generates the corresponding Airflow DAGs at scheduler startup.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Apache Airflow 2.10 | DAG scheduling and monitoring |
| Processing | Apache Spark 3.4.1 | Distributed data transformation |
| Storage Format | Delta Lake 2.4 | ACID-compliant data lake writes |
| Relational Store | PostgreSQL 15 | Production metadata + Airflow DB |
| Document Store | MongoDB 6 | Flexible schema document storage |
| Graph Engine | NetworkX | Lineage graph construction + BFS |
| Containerisation | Docker Compose | Full reproducible local environment |
| Language | Python 3.10+ | All core logic |

---

## Environment Variables

Copy `.env.example` to `.env` and update values before running Docker:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `AIRFLOW_FERNET_KEY` | Encryption key — generate with `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Session signing key — use any random 32-char string |
| `POSTGRES_USER / PASSWORD / DB` | PostgreSQL credentials |
| `MONGO_ROOT_USERNAME / PASSWORD` | MongoDB root credentials |
| `SPARK_MASTER_URL` | Spark master URL (default: `spark://spark-master:7077`) |

---

## Troubleshooting

### Airflow init takes too long or fails
```bash
docker compose logs airflow-init
```
Common fix: wait 60–90 seconds after `docker compose up`. PostgreSQL health-checks retry 10× with 5s delay.

### Port already in use
Check which service is on port 8080:
```bash
sudo lsof -i :8085
```
Airflow webserver uses `8085`, Spark UI uses `8086`, Postgres uses `5433`.

### Clean reset
```bash
docker compose down -v   # removes containers + volumes
docker compose up --build -d
```

---

