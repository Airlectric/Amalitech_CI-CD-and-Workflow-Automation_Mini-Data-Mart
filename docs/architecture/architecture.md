# Architecture Reference

This document provides detailed architectural diagrams for the Mini Data Platform. For the overview, see the [README](../../README.md).

---

## Services Architecture

```mermaid
flowchart TB
    subgraph Docker["Docker Compose"]
        direction TB

        subgraph Core["Core Services"]
            PG[(PostgreSQL 16<br/>Silver, Gold,<br/>Quarantine, Metadata)]
            Redis[(Redis 7<br/>Celery Broker)]
            MinIO["MinIO<br/>Bronze Layer"]
        end

        subgraph Airflow["Apache Airflow 3.x"]
            API["API Server :8080"]
            Sched["Scheduler"]
            Worker["Celery Worker"]
            Trigger["Triggerer"]
            DagProc["DAG Processor"]
            Init["Init (one-shot)"]
        end

        subgraph Monitoring["Observability Stack"]
            StatsD["statsd-exporter<br/>:9102 / :9125"]
            PGExp["postgres-exporter<br/>:9187"]
            RedisExp["redis-exporter<br/>:9121"]
            Prom["Prometheus<br/>:9090"]
            Grafana["Grafana<br/>:3001"]
        end

        subgraph Analytics["Analytics"]
            MB["Metabase :3000"]
            DuckAPI["DuckDB API :8000"]
        end
    end

    %% Airflow -> Core
    API & Sched & Worker & Trigger & DagProc --> PG
    API & Sched & Worker & Trigger & DagProc --> Redis
    Worker --> MinIO

    %% Metrics flow
    API & Sched & Worker & Trigger & DagProc -->|StatsD UDP| StatsD
    StatsD --> Prom
    MinIO -->|/minio/v2/metrics| Prom
    PGExp -->|pg_stat| Prom
    RedisExp -->|redis info| Prom
    PGExp --> PG
    RedisExp --> Redis
    Prom --> Grafana

    %% Analytics
    MB --> PG
    DuckAPI --> MinIO
```

---

## Data Flow Architecture

```mermaid
flowchart TB
    subgraph Bronze["Bronze Layer (MinIO)"]
        Raw["s3://bronze/sales/<br/>ingest_date=YYYY-MM-DD/<br/>*.parquet"]
    end

    subgraph Validation["Two-Tier Validation"]
        T1["Tier 1: Schema Drift<br/>(file-level, DuckDB)"]
        T2["Tier 2: Row Validation<br/>(nulls, ranges, uniqueness)"]
    end

    subgraph Silver["Silver Layer (PostgreSQL)"]
        Sales["silver.sales<br/>(fact)"]
        Customers["silver.customers<br/>(dimension)"]
        Products["silver.products<br/>(dimension)"]
    end

    subgraph QLayer["Quarantine"]
        QTable["quarantine.sales_failed<br/>payload + error_reason"]
        QStatus["Status: pending →<br/>remediated | rejected | dead_letter"]
    end

    subgraph Gold["Gold Layer (PostgreSQL)"]
        Daily["gold.daily_sales"]
        ProdPerf["gold.product_performance"]
        CustAnal["gold.customer_analytics"]
        StorePerf["gold.store_performance"]
        CatIns["gold.category_insights"]
    end

    subgraph Tracking["Audit Trail"]
        Meta["metadata.ingestion_metadata<br/>(file status)"]
        Audit["audit.ingestion_runs<br/>(run counts)"]
        Baselines["metadata.quality_baselines<br/>(drift detection)"]
    end

    Raw --> T1
    T1 -->|schema valid| T2
    T1 -->|schema drift| Meta
    T2 -->|clean rows| Sales
    T2 -->|bad rows| QTable
    T2 --> Meta
    T2 --> Audit

    Sales --> Customers
    Sales --> Products

    Customers & Products --> Daily & ProdPerf & CustAnal & StorePerf & CatIns

    QTable --> QStatus

    style Bronze fill:#CD7F32,color:#fff
    style Silver fill:#C0C0C0,color:#000
    style Gold fill:#FFD700,color:#000
    style QLayer fill:#E74C3C,color:#fff
    style Tracking fill:#9B59B6,color:#fff
```

---

## Data Quality Pipeline

```mermaid
flowchart TB
    subgraph Inputs["Quality Inputs"]
        QT["quarantine.sales_failed"]
        SS["silver.sales"]
        SC["silver.customers"]
        SP["silver.products"]
        BL["metadata.quality_baselines"]
    end

    subgraph Checks["Parallel Quality Checks"]
        C1["validate_quarantine_patterns"]
        C2["profile_silver"]
        C3["detect_drift"]
        C4["check_completeness"]
        C5["check_freshness"]
        C6["check_referential_integrity"]
    end

    subgraph Output["Outputs"]
        PDF["PDF Report<br/>(ReportLab)"]
        Email["Email Alert<br/>(severity-throttled)"]
    end

    QT --> C1
    SS & SC & SP --> C2
    C2 --> C3
    BL --> C3
    SS & SC & SP --> C4
    SS --> C5
    SS & SC & SP --> C6

    C1 & C2 & C3 & C4 & C5 & C6 --> PDF --> Email

    style Checks fill:#27AE60,color:#fff
    style Output fill:#3498DB,color:#fff
```

### Alert Severity Model

| Severity | Throttle | Trigger |
|----------|----------|---------|
| CRITICAL | None (always send) | Schema drift, data loss |
| WARNING | 1 hour | Drift > 10%, high quarantine rate, completeness < 95% |
| INFO | 6 hours | Successful runs, no issues |

---

## Remediation Pipeline

```mermaid
flowchart TD
    Fetch["Fetch pending batch<br/>(WHERE remediation_status = 'pending'<br/>AND retry_count < max_retries<br/>LIMIT batch_size)"]

    Fetch --> Validate["Validate records<br/>(required fields, ranges)"]

    Validate --> Valid{"Fixable?"}

    Valid -->|Yes| BatchUpsert["Batch upsert to silver<br/>(single executemany)"]
    Valid -->|No| BatchReject["Batch reject<br/>(mark as rejected)"]

    BatchUpsert --> TxCommit{"Transaction<br/>committed?"}

    TxCommit -->|Yes| MarkRemediated["Mark remediation_status = 'remediated'"]
    TxCommit -->|No| Rollback["ROLLBACK<br/>retry_count += 1"]

    MarkRemediated --> MoreBatches{"More pending<br/>records?"}
    Rollback --> MoreBatches
    BatchReject --> MoreBatches

    MoreBatches -->|Yes| Fetch
    MoreBatches -->|No| DeadLetter["Escalate<br/>retry_count >= max_retries<br/>→ dead_letter"]

    DeadLetter --> TriggerGold["Trigger silver_to_gold"]
    TriggerGold --> Notify["Send email summary"]

    style BatchUpsert fill:#27AE60,color:#fff
    style BatchReject fill:#E74C3C,color:#fff
    style DeadLetter fill:#8B0000,color:#fff
    style Rollback fill:#E67E22,color:#fff
```

---

## Monitoring Architecture

```mermaid
flowchart LR
    subgraph Airflow["Airflow Services"]
        API["api-server"]
        Sched["scheduler"]
        Worker["worker"]
        Trig["triggerer"]
        DP["dag-processor"]
    end

    subgraph Exporters["Metric Exporters"]
        StatsD["statsd-exporter<br/>:9102"]
        PGE["postgres-exporter<br/>:9187"]
        RE["redis-exporter<br/>:9121"]
    end

    subgraph Storage["MinIO"]
        MinIO["MinIO<br/>/minio/v2/metrics/cluster"]
    end

    Prom["Prometheus<br/>:9090<br/>(scrape every 15s)"]
    Graf["Grafana<br/>:3001<br/>(4 dashboards)"]

    API & Sched & Worker & Trig & DP -->|UDP :9125| StatsD
    StatsD -->|:9102| Prom
    MinIO -->|:9000| Prom
    PGE -->|:9187| Prom
    RE -->|:9121| Prom
    Prom --> Graf
```

### Grafana Dashboards

| Dashboard | Panels | Key Metrics |
|-----------|--------|-------------|
| Airflow Metrics | 8 | Scheduler heartbeat, DAG run duration, task finish rate, executor/pool slots |
| MinIO Metrics | 6 | Bucket size, object count, S3 traffic, request/error rates, disk capacity |
| PostgreSQL Metrics | 6 | Connections, DB size, tuple ops, transactions, cache hit ratio |
| Redis Metrics | 6 | Memory, clients, commands/s, connections, keys, hit ratio |

### StatsD Metric Mappings

| StatsD Pattern | Prometheus Metric | Labels |
|----------------|-------------------|--------|
| `airflow.dagrun.duration.*.*` | `airflow_dagrun_duration_seconds` | `dag_id`, `state` |
| `airflow.dag.*.*.duration` | `airflow_task_duration_seconds` | `dag_id`, `task_id` |
| `airflow.ti.finish.*.*.*` | `airflow_ti_finish_total` | `dag_id`, `task_id`, `state` |
| `airflow.scheduler.heartbeat` | `airflow_scheduler_heartbeat` | -- |
| `airflow.executor.*_slots` | `airflow_executor_slots` | `state` |
| `airflow.pool.*_slots.*` | `airflow_pool_slots` | `pool`, `state` |

---

## CI/CD Pipeline

```mermaid
flowchart LR
    Push["Push / PR to master"] --> Lint["Lint & Type Check<br/>ruff, mypy"]
    Push --> Unit["Unit Tests<br/>pytest + coverage"]
    Push --> Security["Security Scan<br/>bandit, pip-audit"]
    Lint & Unit & Security --> Build["Build & Push<br/>Docker → GHCR"]

    style Lint fill:#3498DB,color:#fff
    style Unit fill:#3498DB,color:#fff
    style Security fill:#3498DB,color:#fff
    style Build fill:#27AE60,color:#fff
```

### CI/CD Jobs

| Job | Tools | Description |
|-----|-------|-------------|
| Lint & Type Check | ruff check, ruff format, mypy | Linting, formatting, type checking |
| Unit Tests | pytest, pytest-cov | DAG imports, generator, remediation logic |
| Security Scan | bandit, pip-audit | Code vulnerability + dependency audit |
| Build & Push | Docker Buildx | Build image, push to GHCR (master only) |

### Schema Reference

| Schema.Table | Key Columns |
|---|---|
| `silver.sales` | sale_id (PK), transaction_id (UK), sale_date, customer_id, product_id, quantity, unit_price, net_amount, processed_at |
| `silver.customers` | customer_id (PK), customer_name, first_purchase_date, total_purchases, total_revenue, customer_segment |
| `silver.products` | product_id (PK), product_name, category, sub_category, avg_unit_price, total_quantity_sold |
| `quarantine.sales_failed` | id + ingestion_run_id (PK), payload (JSONB), error_reason, remediation_status, retry_count, replayed |
| `gold.daily_sales` | sale_date (PK), total_transactions, gross_revenue, net_revenue, unique_customers |
| `gold.product_performance` | product_id (PK), total_revenue, number_of_transactions |
| `gold.customer_analytics` | customer_id (PK), total_revenue, customer_tier, favorite_category |
| `gold.store_performance` | store_location (PK), total_revenue, total_customers_served |
| `gold.category_insights` | category (PK), total_revenue, total_products_sold |
| `metadata.ingestion_metadata` | file_path (PK), dataset_name, status, record_count, checksum |
| `audit.ingestion_runs` | ingestion_run_id (PK), rows_read, rows_written_silver, rows_quarantined, status |
