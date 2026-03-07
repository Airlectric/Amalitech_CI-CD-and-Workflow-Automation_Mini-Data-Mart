# Mini Data Platform

[![Python 3.11](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Airflow 3.x](https://img.shields.io/badge/Airflow-3.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License: Educational](https://img.shields.io/badge/license-educational-green)](./LICENSE)

A production-grade containerized data platform implementing the **Medallion Architecture** (Bronze, Silver, Gold) with automated data quality monitoring, self-healing remediation, and full observability.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Services and Access](#services-and-access)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Data Quality Framework](#data-quality-framework)
- [Remediation Workflow](#remediation-workflow)
- [Monitoring and Observability](#monitoring-and-observability)
- [Database Schema](#database-schema)
- [CI/CD Pipeline](#cicd-pipeline)
- [Metabase Dashboards](#metabase-dashboards)
- [Testing](#testing)
- [Configuration Reference](#configuration-reference)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture

### Core Architecture

![Core Architecture](docs/architecture/MIni%20Data%20Mart%20-%20Core_Architecture.svg)

### Observability

![Observability](docs/architecture/MIni%20Data%20Mart%20-%20Observerability_Diagram.svg)

---

## Tech Stack

| Component | Technology | Port | Purpose |
|-----------|-----------|------|---------|
| Object Storage | MinIO | 9002 / 9003 | Bronze layer (Parquet files) |
| Query Engine | DuckDB | -- | Schema-on-read validation (no data download) |
| Database | PostgreSQL 16 | 5433 | Silver / Gold / Metadata / Quarantine |
| Orchestration | Apache Airflow 3.x | 8080 | DAG scheduling and execution |
| Task Queue | Redis 7 | 6379 | Celery broker for Airflow workers |
| BI Dashboards | Metabase | 3000 | Business analytics |
| Ad-hoc Query | DuckDB API (FastAPI) | 8000 | SQL-on-S3 web interface |
| Metrics | Prometheus | 9090 | Time-series metric storage |
| Visualization | Grafana | 3001 | System monitoring dashboards |
| StatsD Bridge | statsd-exporter | 9102 / 9125 | Airflow StatsD to Prometheus |
| DB Metrics | postgres-exporter | 9187 | PostgreSQL metrics for Prometheus |
| Cache Metrics | redis-exporter | 9121 | Redis metrics for Prometheus |
| CI/CD | GitHub Actions | -- | Lint, test, build, deploy |

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose v2+
- [Git](https://git-scm.com/)
- 8 GB+ RAM recommended

---

## Quick Start

```bash
# Clone
git clone <repo-url>
cd Amalitech_CI-CD-and-Workflow-Automation_Mini-Data-Mart

# Start all services
docker compose up -d --build

# Wait for initialization (~2 minutes)
docker compose ps

# Verify data pipeline
docker compose exec postgres psql -U airflow -d airflow \
  -c "SELECT COUNT(*) FROM silver.sales"
```

---

## Services and Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / airflow |
| Metabase | http://localhost:3000 | Set up on first visit |
| Grafana | http://localhost:3001 | admin / admin (or anonymous) |
| MinIO Console | http://localhost:9003 | minio / minio123 |
| DuckDB API | http://localhost:8000 | -- |
| Prometheus | http://localhost:9090 | -- |
| PostgreSQL | localhost:5433 | airflow / airflow |

---

## Project Structure

```
.
├── .github/workflows/          # CI/CD pipeline definitions
├── config/                     # Monitoring stack configuration
│   ├── grafana/
│   │   ├── dashboards/         # Pre-built Grafana dashboard JSON
│   │   │   ├── airflow-metrics.json
│   │   │   ├── minio-metrics.json
│   │   │   ├── postgres-metrics.json
│   │   │   └── redis-metrics.json
│   │   └── provisioning/       # Auto-provisioning configs
│   │       ├── dashboards/
│   │       └── datasources/
│   ├── prometheus/
│   │   └── prometheus.yml      # Scrape targets
│   └── statsd-exporter/
│       └── mappings.yml        # Airflow metric name mappings
├── dags/                       # Airflow DAG definitions
│   ├── etl/
│   │   ├── generate_sample_data.py
│   │   ├── ingest_bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   ├── data_quality.py
│   │   └── remediation.py
│   └── utils/
│       ├── postgres_hook.py
│       ├── minio_hook.py
│       ├── duckdb_utils.py
│       └── email_utils.py
├── scripts/
│   ├── data_generator/         # Synthetic data generator
│   ├── postgres_init/          # Database DDL (init.sql)
│   └── init-minio.sh           # MinIO bucket creation
├── services/
│   └── duckdb-api/             # FastAPI ad-hoc query service
├── tests/                      # Unit, integration, E2E tests
├── docs/                       # Architecture docs, SQL queries
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env
```

---

## Data Pipeline

### DAG Overview

| DAG | Schedule | Trigger | Description |
|-----|----------|---------|-------------|
| `generate_sample_data` | `0 6,12,18 * * *` | Scheduled | Generates 1000 rows per mode to MinIO |
| `ingest_bronze_to_silver` | `0 */6 * * *` | Scheduled | Two-tier validation, split good/bad rows |
| `silver_to_gold` | None | Triggered by ingestion / remediation | Builds star schema (dimensions then facts) |
| `data_quality_checks` | `0 */6 * * *` | Scheduled | 6 parallel quality checks, PDF report |
| `remediation_workflow` | None | Manual | Batch fix and replay quarantined records |

### DAG Dependency Graph

```mermaid
flowchart LR
    A["generate_sample_data<br/><i>3x daily</i>"] -->|Parquet to MinIO| B

    subgraph Ingestion
        B["ingest_bronze_to_silver<br/><i>every 6h</i>"]
    end

    B -->|TriggerDagRunOperator| C["silver_to_gold"]

    subgraph Quality
        D["data_quality_checks<br/><i>every 6h</i>"]
    end

    subgraph Remediation
        E["remediation_workflow<br/><i>manual</i>"]
    end

    E -->|TriggerDagRunOperator| C
    B -.->|data available| D
```

### Medallion Architecture

```
Bronze (MinIO)                    Silver (PostgreSQL)              Gold (PostgreSQL)
─────────────────                 ─────────────────────            ──────────────────
s3://bronze/sales/                silver.sales (fact)              gold.daily_sales
  ingest_date=YYYY-MM-DD/        silver.customers (dim)           gold.product_performance
    <uuid>.parquet                silver.products (dim)            gold.customer_analytics
                                                                   gold.store_performance
                                                                   gold.category_insights
                                                                   gold.v_monthly_sales (view)
                                                                   gold.v_regional_sales (view)
```

### Two-Tier Validation

**Tier 1 -- File-Level (Schema Drift Detection)**

Compares actual Parquet columns against expected spec. If there is a mismatch (missing columns, extra columns, type changes), the entire file is rejected with status `SCHEMA_DRIFT` in `metadata.ingestion_metadata`. The file is never re-read on subsequent runs.

**Tier 2 -- Row-Level Validation**

For schema-valid files, each row is checked for:
- Required columns: `transaction_id`, `sale_date`, `customer_id`, `product_id`, `quantity`, `unit_price`, `net_amount`
- Value ranges: quantity [1, 1000], unit_price [0, 100000], discount_percentage [0, 100]
- Rows failing validation are tagged with `error_reason` (e.g., `null:customer_id;range:quantity`) and sent to quarantine with the full payload preserved as JSONB.

---

## Data Quality Framework

Six checks run in parallel every 6 hours:

| Check | What It Validates | Threshold |
|-------|-------------------|-----------|
| Quarantine Patterns | No NULL ids/payloads/errors in quarantine table | 0 nulls |
| Silver Profiling | Row counts across `sales`, `customers`, `products` | Tables populated |
| Drift Detection | Current row count vs 24-hour baseline | < 10% change |
| Completeness | Non-null percentage for critical columns | >= 95% |
| Freshness | Time since last `processed_at` in `silver.sales` | <= 24 hours |
| Referential Integrity | Orphan `customer_id` / `product_id` in sales | 0 orphans |

**Outputs:**
- PDF report generated via ReportLab with all check results
- Email alert with PDF attachment (severity-throttled: CRITICAL immediate, WARNING 1h, INFO 6h)

---

## Remediation Workflow

The `remediation_workflow` DAG processes quarantined records with production-grade patterns:

```mermaid
flowchart TD
    Start["Fetch pending batch<br/>(configurable size)"] --> Validate["Validate records"]

    Validate --> Valid{"Fixable?"}
    Valid -->|Yes| Batch["Batch upsert to silver<br/>(single transaction)"]
    Valid -->|No| Reject["Batch reject<br/>(mark as rejected)"]

    Batch --> Mark["Mark as remediated"]
    Batch -->|Transaction failed| Retry["Increment retry_count"]
    Reject --> More{"More batches?"}
    Mark --> More
    Retry --> More

    More -->|Yes| Start
    More -->|No| DeadLetter["Escalate records with<br/>retry_count >= max to dead-letter"]

    DeadLetter --> Trigger["Trigger silver_to_gold<br/>rebuild"]
    Trigger --> Notify["Send email notification"]
```

**Key design decisions:**

| Concern | Implementation |
|---------|---------------|
| Batch operations | All valid records upserted in one `executemany` call, not row-by-row |
| Transactional consistency | Silver insert + quarantine update in a single `COMMIT`; `ROLLBACK` on failure |
| Status tracking | `remediation_status` enum: `pending` / `remediated` / `rejected` / `dead_letter` |
| Dead-letter escalation | After `retry_count >= max_retries` (default 3), records move to `dead_letter` |
| Pagination | Processes batches until no pending records remain |
| Configurable | `remediation_batch_size` and `remediation_max_retries` via Airflow Variables |
| Gold rebuild | `TriggerDagRunOperator` fires `silver_to_gold` after remediation completes |

---

## Screenshots

### Airflow DAGs
![Airflow DAGs](docs/screenshots/airflow_dags.jpg)

### MinIO Bronze Layer
![MinIO Bronze Bucket](docs/screenshots/minio_bronze_bucket_with_paquet_files.jpg)

### DuckDB API
![DuckDB API](docs/screenshots/duckdb_api_for_querying_minio_objects.jpg)

### Grafana Dashboards

![Grafana Dashboards List](docs/screenshots/grafana_dashboards_list.jpg)

#### Airflow Metrics
![Grafana Airflow Metrics](docs/screenshots/grafana_airflow_metrics.jpg)

#### MinIO Metrics
![Grafana MinIO Metrics](docs/screenshots/grafana_minio_metrics.jpg)

#### PostgreSQL Metrics
![Grafana PostgreSQL Metrics](docs/screenshots/grafana_postgres_metrics.jpg)

#### Redis & Celery Metrics
![Grafana Redis Celery Metrics](docs/screenshots/grafana_redis_celery_metrics.jpg)

### Prometheus

![Prometheus Exporters](docs/screenshots/prometheus_list_of_exporters.jpg)
![Prometheus Targets Health](docs/screenshots/prometheus_targets_health.jpg)

### Metabase Dashboards

![Metabase Questions & Dashboards](docs/screenshots/metabase_list_of_questions_and_viz_dashboards.jpg)

#### Overview
![Metabase Overview](docs/screenshots/metabase_dashboard_overview.jpg)

#### Executive Summary
![Metabase Executive Summary](docs/screenshots/metabase_dashboard_exec_summary.jpg)

#### Sales Overview
![Metabase Sales Overview](docs/screenshots/metabase_dashboard_sales_overview.jpg)

#### Product Analytics
![Metabase Product Analytics](docs/screenshots/metabase_dashboard_product_overview.jpg)

#### Customer Analytics
![Metabase Customer Analytics](docs/screenshots/metabase_dashboard_customer_overview.jpg)

#### Store Performance
![Metabase Store Performance](docs/screenshots/metabase_dashboard_store_performance.jpg)

### Email Alerts

#### Data Quality Report
![Data Quality Email Report](docs/screenshots/data_quality_email_report.jpg)

#### Schema Drift Alert
![Schema Drift Alert](docs/screenshots/schema_drift_email_alert_during_ingestion.jpg)

---

## Monitoring and Observability

### Metrics Pipeline

```
Airflow (scheduler/worker/api-server/triggerer/dag-processor)
    │  StatsD UDP :9125
    v
statsd-exporter (:9102)  ──┐
MinIO (/minio/v2/metrics)  ├──> Prometheus (:9090) ──> Grafana (:3001)
postgres-exporter (:9187)  │
redis-exporter (:9121)   ──┘
```

All five Airflow services emit StatsD metrics. Prometheus scrapes four exporters every 15 seconds. Grafana auto-provisions four dashboards on startup.

### Grafana Dashboards

| Dashboard | Key Panels |
|-----------|-----------|
| **Airflow Metrics** | Scheduler heartbeat rate, loop duration, DAG run duration, task finish rate, executor slots, pool usage |
| **MinIO Metrics** | Bucket size, object count, S3 traffic rate, request rate by API, error rate, cluster disk capacity |
| **PostgreSQL Metrics** | Active connections, connection utilization, database size, tuple operations rate, transaction commits/rollbacks, cache hit ratio |
| **Redis Metrics** | Memory usage, connected/blocked clients, commands processed rate, connection rate, keys per database, keyspace hit ratio |

### Verification

```bash
# Check Prometheus targets are up
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool

# Query a metric
curl -s 'http://localhost:9090/api/v1/query?query=airflow_scheduler_heartbeat'

# Open Grafana (anonymous access enabled)
open http://localhost:3001
```

### Metabase Dashboards

A pre-configured Metabase dashboard with 7 tabs is auto-provisioned on first startup:

| Tab | Content |
|-----|---------|
| **Overview** | 6 KPIs + daily/weekly/monthly trends + top products/customers/stores |
| **Executive Summary** | High-level KPIs + daily sales trend |
| **Sales Overview** | Daily sales + growth analysis + breakdowns by product/category/store |
| **Product Analytics** | Top products + category breakdowns + detailed performance |
| **Customer Analytics** | Customer overview + tier distribution + full details |
| **Store Performance** | Store comparisons + revenue by region |
| **Data Quality** | Quarantine KPIs + error types + failed records trend + remediation status |

**How it works:** On first PostgreSQL startup (empty volume), `scripts/metabase_init/restore_metabase.sh` restores the dashboard backup (`metabase_backup.sql`) into the `metabase` database. Metabase reads from this database on boot.

**Re-exporting dashboards** (after making changes in the UI):
```bash
# Export current Metabase state
docker compose exec postgres pg_dump -U airflow metabase > scripts/metabase_init/metabase_backup.sql

# Commit the updated backup
git add scripts/metabase_init/metabase_backup.sql
```

**Access:** http://localhost:3000

All dashboard queries are documented in [`docs/dashboard_queries.sql`](docs/dashboard_queries.sql).

---

## Database Schema

### Star Schema (ER Diagram)

![ER Diagram](docs/architecture/db_schema_diagrams/MIni%20Data%20Mart%20-%20ER_diagram.svg)

### Metadata & Audit (ER Diagram)

![Metadata ER Diagram](docs/architecture/db_schema_diagrams/Mini_Mart_Metadata%20ER_Diagrams.svg)

### Schema Overview

| Schema | Table | Purpose |
|--------|-------|---------|
| `silver` | `sales` | Cleaned transaction fact table |
| `silver` | `customers` | Customer dimension (derived from sales) |
| `silver` | `products` | Product dimension (derived from sales) |
| `gold` | `daily_sales` | Daily aggregated metrics |
| `gold` | `product_performance` | Product-level KPIs |
| `gold` | `customer_analytics` | Customer segments and behavior |
| `gold` | `store_performance` | Store-level metrics |
| `gold` | `category_insights` | Category aggregates |
| `gold` | `v_monthly_sales` | Monthly trends (view) |
| `gold` | `v_regional_sales` | Regional performance (view) |
| `quarantine` | `sales_failed` | Failed records with payload, error, retry tracking |
| `metadata` | `ingestion_metadata` | File processing status (PROCESSED / SCHEMA_DRIFT / FAILED) |
| `metadata` | `quality_baselines` | Historical metrics for drift detection |
| `audit` | `ingestion_runs` | Per-run audit trail (rows read/written/quarantined) |

### Column Reference

**silver.sales**: sale_id (PK), transaction_id (UK), sale_date, sale_hour, customer_id, customer_name, product_id, product_name, category, sub_category, quantity, unit_price, discount_percentage, discount_amount, gross_amount, net_amount, profit_margin, payment_method, payment_category, store_location, region, is_weekend, is_holiday, ingest_date, source_file, processed_at

**silver.customers**: customer_id (PK), customer_name, first_purchase_date, total_purchases, total_revenue, average_order_value, customer_segment, last_purchase_date, days_since_last_purchase, created_at

**silver.products**: product_id (PK), product_name, category, sub_category, min_unit_price, max_unit_price, avg_unit_price, total_quantity_sold, total_revenue, created_at, updated_at

**quarantine.sales_failed**: id + ingestion_run_id (composite PK), payload (JSONB), error_reason, failed_at, source_file, corrected_by, corrected_at, replayed, replayed_at, remediation_status, retry_count

**gold.daily_sales**: sale_date (PK), total_transactions, total_quantity_sold, gross_revenue, total_discounts, net_revenue, average_order_value, unique_customers, unique_products, top_category, created_at

**gold.product_performance**: product_id (PK), product_name, category, total_quantity_sold, total_revenue, average_unit_price, total_discount_given, number_of_transactions, average_quantity_per_transaction, created_at

**gold.customer_analytics**: customer_id (PK), customer_name, total_purchases, total_revenue, average_order_value, favorite_category, favorite_payment_method, most_visited_store, customer_tier, created_at

**gold.store_performance**: store_location (PK), region, total_transactions, total_revenue, average_order_value, total_customers_served, top_selling_category, created_at

**gold.category_insights**: category (PK), total_products_sold, total_revenue, average_discount_percentage, number_of_transactions, average_order_value, created_at

**metadata.ingestion_metadata**: file_path (PK), dataset_name, checksum, ingest_date, status, record_count, processed_at, airflow_run_id, error_message, created_at

**metadata.quality_baselines**: id (PK), table_name, metric_name, metric_value, recorded_at

**audit.ingestion_runs**: ingestion_run_id (PK, UUID), started_at, finished_at, files_scanned (TEXT[]), rows_read, rows_written_silver, rows_quarantined, status, operator

---

## CI/CD Pipeline

```mermaid
flowchart TD
    Trigger["Push / PR to master<br/>or manual dispatch"]

    Trigger --> Lint
    Trigger --> Tests
    Trigger --> Security

    subgraph CI["CI: Parallel Jobs"]
        Lint["Lint & Type Check<br/>ruff check · ruff format · mypy"]
        Tests["Unit Tests<br/>pytest · pytest-cov"]
        Security["Security Scan<br/>bandit · pip-audit"]
    end

    Lint --> Gate{All passed?}
    Tests --> Gate
    Security --> Gate

    Gate -->|Yes + push to master| Build["Build & Push<br/>Docker image → GHCR"]
    Gate -->|PR or failure| Stop["Done"]

    Build --> Deploy["CD: Deploy to Test<br/>docker compose up · schema validation"]
    Build --> Validate["Data Flow Validation<br/>MinIO → Airflow → PostgreSQL → Metabase"]

    style Lint fill:#2563eb,color:#fff
    style Tests fill:#16a34a,color:#fff
    style Security fill:#dc2626,color:#fff
    style Build fill:#7c3aed,color:#fff
    style Deploy fill:#0891b2,color:#fff
    style Validate fill:#ca8a04,color:#000
```

| Job | Tools | What It Does |
|-----|-------|-------------|
| **Lint & Type Check** | `ruff check`, `ruff format --check`, `mypy` | Enforces code style (PEP 8), import ordering, formatting consistency, and static type analysis |
| **Unit Tests** | `pytest`, `pytest-cov` | Runs 51 unit tests covering DAG structure, data generator modes, remediation logic, and task functions |
| **Security Scan** | `bandit`, `pip-audit` | Scans source code for common vulnerabilities (SQL injection, hardcoded secrets) and checks dependencies for known CVEs |
| **Build & Push** | Docker Buildx | Builds the Airflow image and pushes to GitHub Container Registry (only on push to `master`, not on PRs) |
| **Deploy to Test** | `docker compose` | Spins up all services, verifies containers are healthy, validates all 5 database schemas exist |
| **Data Flow Validation** | Airflow CLI, `psql` | Triggers DAGs in order (generate → ingest → gold), validates data flows from MinIO → Silver → Gold → Metabase |

**Workflow features:** `workflow_dispatch` for manual triggers, concurrency groups to cancel stale runs, pip caching, JUnit XML test output.

### Pre-commit Hooks

Pre-commit hooks run the same checks locally before each commit, catching issues before they reach CI.

```bash
# Install pre-commit
pip install pre-commit

# Set up hooks (one-time)
pre-commit install

# Run manually against all files
pre-commit run --all-files
```

The `.pre-commit-config.yaml` includes: ruff (lint + format), bandit (security), and mypy (types).

---

## Metabase Dashboards

Seven pre-configured dashboards connect to the Gold layer:

| Dashboard | Key Metrics |
|-----------|-----------|
| Main Dashboard | Unified KPIs across all domains |
| Executive Summary | Revenue, transactions, customer count |
| Sales Overview | Daily trends, payment methods, weekend vs weekday |
| Product Analytics | Product revenue, category performance |
| Customer Analytics | Segments, lifetime value, tier distribution |
| Store Performance | Revenue by location, top stores |
| Data Quality | Quarantine trends, error types, quality score |

**Setup:** Open http://localhost:3000, connect to PostgreSQL (`host: postgres`, `port: 5432`, `db: airflow`, `user: airflow`, `pass: airflow`). See `docs/dashboard_queries.sql` for all 40+ queries.

---

## Testing

### Test Architecture

Tests are organized into three categories using pytest markers:

| Marker | Category | Requires Docker? | What It Covers |
|--------|----------|:-:|----------------|
| *(none)* | Unit tests | No | Pure logic tests — no database, no network, all dependencies mocked |
| `@pytest.mark.integration` | Integration tests | Yes | Live database queries, container health checks, schema validation |
| `@pytest.mark.slow` | End-to-end tests | Yes | Full pipeline flows across multiple services |

### Test Suites

#### `tests/test_dags.py` — DAG Structure & Task Logic (18 tests)

Validates that all five Airflow DAGs load correctly and have the expected configuration:

- **Import tests**: Each DAG file (`data_quality`, `ingest_bronze_to_silver`, `generate_sample_data`, `remediation`, `silver_to_gold`) can be imported without errors
- **DAG ID tests**: Verifies each DAG has the correct `dag_id`
- **Task tests**: Checks that `data_quality_checks` DAG contains all 8 expected tasks (`validate_quarantine_patterns`, `profile_silver`, `detect_drift`, `check_completeness`, `check_freshness`, `check_referential_integrity`, `generate_data_docs`, `send_alerts`)
- **Config tests**: Validates `default_args` (owner, retries, email_on_failure), schedule intervals, start dates, and tags
- **Task logic tests**: Mocks `PostgresLayerHook` and runs `validate_quarantine_patterns`, `profile_silver`, and `detect_drift` functions to verify return values

#### `tests/test_data_generator.py` — Data Generator (12 tests)

Tests the synthetic data generator (`scripts/data_generator/generator.py`) that produces Parquet files for the Bronze layer:

- **Mode tests**: Validates each generation mode produces correct output:
  - `clean` — No nulls in required fields, positive quantity and price
  - `dirty` — Contains deliberate null values for quarantine testing
  - `duplicates` — Produces duplicate `transaction_id` values
  - `mixed` — Combination of clean and dirty rows
  - `edge_cases` — Boundary values and special characters
  - `schema_invalid` — Columns that don't match the expected schema
- **Edge cases**: Zero rows (empty DataFrame), single row, large dataset (10,000 rows)
- **Column validation**: Verifies column order matches the expected schema (`transaction_id`, `sale_date`, `sale_hour`, `customer_id`, ...)

#### `tests/test_remediation.py` — Remediation Workflow (14 tests)

Tests the quarantine record remediation pipeline:

- **DAG structure**: Verifies `remediation_workflow` DAG has `schedule=None` (manual trigger only) and contains tasks `remediate_all_batches`, `send_remediation_notification`, `trigger_gold_rebuild`
- **`_validate_records()`**: Tests record validation logic:
  - Valid records with all required fields pass validation
  - Records missing required fields (`sale_date`, `product_id`, etc.) are rejected
  - Negative quantity or negative price records are rejected
  - Malformed JSON payloads are caught and rejected
- **`_batch_replay()`**: Tests the Silver table upsert logic:
  - Empty input returns zero counts
  - Successful replay calls `conn.commit()`
  - Database errors trigger `conn.rollback()` and increment failure count
- **`_batch_reject()`**: Tests quarantine status updates (marks records as `rejected`)
- **`_get_quarantine_stats()`**: Tests statistics aggregation (total, pending, remediated, rejected, dead_letter counts)
- **Integration tests** *(marked `@pytest.mark.integration`)*: Validates `quarantine.sales_failed` table exists and has required columns

#### `tests/test_integration.py` — Infrastructure & Pipeline (15 tests)

End-to-end tests that verify the live Docker environment:

- **PostgreSQL**: Connection, schema existence (`silver`, `quarantine`, `bronze`), table structure validation
- **MinIO**: Container health check, bucket existence
- **Airflow**: Container health, DAG loading (active DAGs in database), specific DAG existence
- **Data quality pipeline**: Quarantine table structure, Silver table columns, replay functionality
- **End-to-end flow**: Bronze-to-Silver data flow verification, quarantine record lifecycle
- **Docker Compose**: All required containers running (`postgres`, `airflow`, `redis`, `minio`)

### Running Tests

```bash
# ─── Unit tests (no Docker required) ───
# Run all unit tests
pytest tests/ -m "not integration" -v

# Run a specific test suite
pytest tests/test_data_generator.py -v
pytest tests/test_remediation.py -v
pytest tests/test_dags.py -v

# Run with coverage report
pytest tests/ -m "not integration" --cov=dags --cov=scripts --cov-report=term-missing

# ─── Integration tests (requires running Docker services) ───
# Start services first
docker compose up -d --build

# Run integration tests from the host (connects to localhost:5433)
pytest tests/ -m integration -v

# Run all tests inside the Airflow container
docker compose exec airflow-worker pytest tests/ -v

# ─── Linting & security (same checks as CI) ───
ruff check dags/ scripts/ tests/
ruff format --check dags/ scripts/ tests/
bandit -r dags/ scripts/ -c pyproject.toml -ll
```

### Test Configuration

Tests are configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --tb=short"
markers = [
    "unit: Unit tests",
    "integration: Integration tests requiring Docker",
    "slow: Slow running tests",
    "dag: Tests for Airflow DAGs",
]
```

The `tests/conftest.py` file handles:
- **Path resolution**: Adds `dags/`, `dags/etl/`, `scripts/data_generator/` to `sys.path` so test imports work outside the Airflow container
- **Namespace isolation**: Clears cached `utils` module between test files to prevent `dags/utils` vs `scripts/utils` conflicts
- **Shared fixtures**: `mock_postgres_connection`, `sample_sales_data`, `sample_quarantine_data`

---

## Configuration Reference

### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `airflow` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `airflow` | PostgreSQL password |
| `MINIO_ROOT_USER` | `minio` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minio123` | MinIO secret key |
| `GRAFANA_PORT` | `3001` | Grafana host port |
| `AIRFLOW_UID` | `50000` | Airflow container user ID |

### Airflow Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `alert_email` | `daniel.doe@a2sv.org` | Alert recipient |
| `remediation_batch_size` | `1000` | Records per remediation batch |
| `remediation_max_retries` | `3` | Max retries before dead-letter |

### Monitoring Ports

| Port | Protocol | Service |
|------|----------|---------|
| 9125 | UDP | StatsD ingest (Airflow -> statsd-exporter) |
| 9102 | HTTP | statsd-exporter metrics (Prometheus scrape) |
| 9090 | HTTP | Prometheus UI and API |
| 9187 | HTTP | postgres-exporter metrics |
| 9121 | HTTP | redis-exporter metrics |
| 3001 | HTTP | Grafana UI |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "feat: add my feature"`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

---

## License

This project is for educational purposes as part of the Amalitech DEM012 CI/CD and Workflow Automation module.
