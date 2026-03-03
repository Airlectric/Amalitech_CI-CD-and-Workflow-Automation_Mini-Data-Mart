# Mini Data Platform

[![CI/CD Pipeline](https://github.com/${{ github.repository }}/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/${{ github.repository }}/actions/workflows/ci-cd.yml)
[![Python Version](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue)](https://www.docker.com/)

> A production-grade containerized data platform using Docker Compose that implements medallion architecture (Bronze → Silver → Gold) with Star Schema, CI/CD automation, and data quality monitoring.

## Architecture Overview

```
Data Generator → MinIO (Bronze) → Airflow + DuckDB → PostgreSQL (Silver/Gold) → Metabase
                                            ↓
                              Data Quality (SQL-based) + PDF Reports
                                            ↓
                              Remediation Workflow (Auto-fix)
```

## New Features (Latest Updates)

- ✅ **Silver Layer Population**: Customers & Products now populated during `ingest_bronze_to_silver`
- ✅ **Comprehensive Data Quality**: Validates all 3 Silver tables (sales, customers, products)
- ✅ **SQL-Based Quality Checks**: No external dependencies - pure SQL queries for validation
- ✅ **PDF Reports**: Data quality reports generated as PDF and attached to emails
- ✅ **Remediation Workflow**: Automatic fixing and replaying of quarantined records
- ✅ **CI/CD Pipeline**: GitHub Actions with unit, integration, and E2E tests
- ✅ **Metabase Dashboards**: 7 pre-configured dashboards with KPIs and visualizations
- ✅ **Data Quality Dashboard**: Real-time monitoring of quarantine trends and remediation status

## Star Schema Design

This platform implements a proper **Star Schema** for data warehousing.

```mermaid
erDiagram
    SILVER_SALES {
        int sale_id PK
        string transaction_id UK
        date sale_date
        int sale_hour
        string customer_id FK
        string product_id FK
        string store_location FK
        int quantity
        decimal unit_price
        decimal net_amount
    }
    SILVER_CUSTOMERS {
        string customer_id PK
        string customer_name
        date first_purchase_date
        int total_purchases
        decimal total_revenue
        string customer_segment
    }
    SILVER_PRODUCTS {
        string product_id PK
        string product_name
        string category
        string sub_category
    }
    GOLD_DAILY_SALES {
        date sale_date PK
        int total_transactions
        int total_quantity_sold
        decimal gross_revenue
        decimal net_revenue
    }
    GOLD_PRODUCT_PERFORMANCE {
        string product_id PK
        string product_name
        string category
        decimal total_revenue
    }
    GOLD_CUSTOMER_ANALYTICS {
        string customer_id PK
        string customer_name
        int total_purchases
        decimal total_revenue
        string customer_tier
    }
    GOLD_STORE_PERFORMANCE {
        string store_location PK
        string region
        decimal total_revenue
    }
    GOLD_CATEGORY_INSIGHTS {
        string category PK
        int total_products_sold
        decimal total_revenue
    }

    SILVER_SALES }o--|| SILVER_CUSTOMERS : "customer_id"
    SILVER_SALES }o--|| SILVER_PRODUCTS : "product_id"
```

### All Tables

#### Silver Layer (Schema: `silver`)

| Table | Type | Description |
|-------|------|-------------|
| `sales` | Fact | Transaction records (populated from Bronze) |
| `customers` | Dimension | Customer profiles (populated from Bronze during ingest) |
| `products` | Dimension | Product catalog (populated from Bronze during ingest) |

#### Gold Layer (Schema: `gold`)

| Table | Type | Primary Key | Description |
|-------|------|-------------|-------------|
| `daily_sales` | Fact | `sale_date` | Daily aggregated metrics |
| `product_performance` | Fact | `product_id` | Product analytics |
| `customer_analytics` | Fact | `customer_id` | Customer behavior |
| `store_performance` | Fact | `store_location` | Store metrics |
| `category_insights` | Fact | `category` | Category aggregates |
| `v_monthly_sales` | View | — | Monthly aggregated view |
| `v_regional_sales` | View | — | Regional sales view |

### Quarantine Schema

| Table | Type | Description |
|-------|------|-------------|
| `sales_failed` | Quarantine | Failed records pending remediation |

## Tech Stack

| Component       | Technology       | Port      | Purpose                          |
|-----------------|------------------|-----------|----------------------------------|
| Data Lake       | MinIO            | 9002/9003 | Bronze layer (Parquet files)     |
| Query Engine    | DuckDB           | —         | Schema-on-read validation        |
| Database        | PostgreSQL 16    | 5433      | Silver/Gold layers + Metadata    |
| Orchestration   | Apache Airflow   | 8080      | ETL pipeline scheduling          |
| Visualization   | Metabase         | 3000      | BI dashboards & reporting        |
| CI/CD           | GitHub Actions   | —         | Automated pipelines              |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose v2+
- [Git](https://git-scm.com/)
- 8GB+ RAM recommended

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd Amalitech_CI-CD-and-Workflow-Automation_Mini-Data-Mart

# 2. Start all services
docker compose up -d --build

# 3. Wait for services to initialize (~2 minutes)
docker compose ps

# 4. Access the services (see table below)
```

## Services & Credentials

| Service         | URL                          | Credentials             |
|-----------------|------------------------------|------------------------|
| Airflow UI      | http://localhost:8080        | admin / airflow        |
| Metabase        | http://localhost:3000        | Set up on first visit  |
| MinIO Console   | http://localhost:9003        | minio / minio123       |
| PostgreSQL      | localhost:5433               | airflow / airflow      |

## Project Structure

```
.
├── .github/workflows/       # CI/CD pipeline definitions
├── dags/                    # Airflow DAG definitions
│   ├── etl/
│   │   ├── data_quality.py           # Data quality checks (SQL-based)
│   │   ├── ingest_bronze_to_silver.py # Bronze → Silver + dimension population
│   │   ├── silver_to_gold.py         # Silver → Gold (Star Schema)
│   │   ├── generate_sample_data.py    # Auto data generation
│   │   └── remediation.py             # Auto-fix quarantined records
│   └── utils/
│       ├── minio_hook.py              # MinIO operations
│       ├── postgres_hook.py           # PostgreSQL operations
│       └── duckdb_utils.py           # DuckDB validation
├── data/                     # Data storage (local)
├── docs/                     # Documentation
│   ├── architecture.md       # Architecture diagrams
│   └── dashboard_queries.sql # All dashboard SQL queries
├── notes/                    # User guides
│   └── METABASE_SETUP_GUIDE.md # Metabase dashboard creation guide
├── scripts/                  # Utility scripts
│   ├── data_generator/       # Parquet data generator
│   ├── postgres_init/       # Database initialization
│   ├── utils/              # Email, hooks utilities
│   └── setup_metabase.py   # Metabase setup script
├── tests/                   # Unit & Integration tests
│   ├── test_dags.py
│   ├── test_data_generator.py
│   ├── test_remediation.py
│   ├── test_integration.py
│   └── conftest.py
├── docker-compose.yml       # All services definition
├── Dockerfile              # Airflow custom image
├── requirements.txt        # Python dependencies
├── pytest.ini              # Pytest configuration
├── .env                   # Environment variables
└── README.md
```

## Data Flow & Pipelines

```mermaid
flowchart TD
    A[Data Generator] -->|Parquet Files| B[MinIO Bronze]
    
    B -->|Every 6hrs| C[ingest_bronze_to_silver]
    
    C -->|DuckDB Validation| D{Valid?}
    D -->|Yes| E[PostgreSQL Silver]
    D -->|No| F[Quarantine]
    
    E -->|Populate| G[silver.sales]
    E -->|Extract| H[silver.customers]
    E -->|Extract| I[silver.products]
    
    G -->|Quality Check| J[data_quality_checks]
    H -->|Quality Check| J
    I -->|Quality Check| J
    
    J -->|Generate PDF| K[Email Report]
    J -->|Drift Detected| L[Alert]
    
    F -->|Manual/Auto| M[remediation_workflow]
    M -->|Fix & Replay| E
    
    G -->|6hrs| N[silver_to_gold]
    N -->|Aggregations| O[Gold Tables]
    
    O --> P[Metabase Dashboards]
```

### DAGs & Scheduling

| DAG | Schedule | Description |
|-----|----------|-------------|
| `generate_sample_data` | 6am, 12pm, 6pm | Generates 1000 rows to MinIO |
| `ingest_bronze_to_silver` | Every 6 hours | Bronze → Silver with validation + dimension population |
| `data_quality_checks` | Daily 6am | SQL-based quality checks on all Silver tables |
| `silver_to_gold` | After quality | Builds Star Schema (dimensions + facts) |
| `remediation_workflow` | Manual | Fix and replay quarantined records |

### Data Quality Features

- **SQL-Based Validation**: No external dependencies
- **Quarantine Pattern Validation**: Checks for NULL values in quarantined records
- **Profiling**: Row counts and uniqueness for all Silver tables
- **Drift Detection**: Monitors data volume changes (>10% triggers alert)
- **PDF Reports**: Generated and attached to email alerts

### Remediation Features

- **Automatic Validation**: Checks required fields and data types
- **Auto-Rejection**: Invalid records marked as reviewed
- **Replay**: Fixed records re-inserted to Silver
- **Statistics**: Full tracking of remediated vs rejected records

## Database Schema

### Silver Layer

**Fact Table: `silver.sales`**
| Column | Type | Description |
|--------|------|-------------|
| sale_id | SERIAL | Primary key |
| transaction_id | VARCHAR(50) | Unique transaction ID |
| sale_date | DATE | Date of sale |
| customer_id | VARCHAR(50) | FK to customers |
| product_id | VARCHAR(50) | FK to products |
| quantity | INTEGER | Units sold |
| net_amount | DECIMAL | Revenue after discount |
| ... | ... | Other sales fields |

**Dimension Table: `silver.customers`**
| Column | Type | Description |
|--------|------|-------------|
| customer_id | VARCHAR(50) | Primary key |
| customer_name | VARCHAR(100) | Full name |
| first_purchase_date | DATE | First transaction |
| total_purchases | INTEGER | Transaction count |
| total_revenue | DECIMAL | Lifetime value |
| customer_segment | VARCHAR(20) | Bronze/Silver/Gold/Platinum |

**Dimension Table: `silver.products`**
| Column | Type | Description |
|--------|------|-------------|
| product_id | VARCHAR(50) | Primary key |
| product_name | VARCHAR(100) | Product name |
| category | VARCHAR(50) | Product category |
| sub_category | VARCHAR(50) | Product sub-category |
| min/max/avg_unit_price | DECIMAL | Price statistics |
| total_quantity_sold | INTEGER | Units sold |
| total_revenue | DECIMAL | Total revenue |

### Quarantine Layer

**Table: `quarantine.sales_failed`**
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| payload | JSONB | Original record data |
| error_reason | VARCHAR(500) | Why it failed validation |
| source_file | VARCHAR(255) | Source file name |
| failed_at | TIMESTAMP | When it failed |
| replayed | BOOLEAN | Has been remediated |
| replayed_at | TIMESTAMP | When it was remediated |
| corrected_by | VARCHAR(50) | Who fixed it |

### Gold Layer

| Table | Primary Key | Description |
|-------|-------------|-------------|
| daily_sales | sale_date | Daily aggregated metrics |
| product_performance | product_id | Product-level analytics |
| customer_analytics | customer_id | Customer behavior analysis |
| store_performance | store_location | Store-level metrics |
| category_insights | category | Category aggregates |

## Running the Pipeline

```bash
# Generate and upload data to MinIO (or wait for scheduled run)
docker compose exec airflow-worker python scripts/data_generator/generator.py

# Trigger DAGs via Airflow UI at http://localhost:8080
# Or via CLI:
docker compose exec airflow-worker airflow dags trigger generate_sample_data
docker compose exec airflow-worker airflow dags trigger ingest_bronze_to_silver
docker compose exec airflow-worker airflow dags trigger data_quality_checks
docker compose exec airflow-worker airflow dags trigger silver_to_gold
docker compose exec airflow-worker airflow dags trigger remediation_workflow

# Verify data
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM silver.sales"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM silver.customers"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM silver.products"
```

## Testing

```bash
# Run unit tests (in Docker)
docker exec airflow-worker python /opt/airflow/scripts/run_tests.py

# Run pytest directly
docker exec airflow-worker pytest tests/ -v

# Run specific test file
docker exec airflow-worker pytest tests/test_remediation.py -v
```

### Test Coverage

- **Unit Tests**: Data generator, DAG imports, task logic
- **Integration Tests**: PostgreSQL, MinIO connectivity
- **E2E Tests**: Full pipeline validation

## CI/CD Pipeline

The project includes GitHub Actions workflows for:

```mermaid
flowchart LR
    A[Push/PR] --> B{Build & Test}
    B -->|Lint| C[flake8, black, mypy]
    B -->|Unit| D[pytest]
    B -->|Security| E[safety, bandit]
    C --> F[Build Docker]
    D --> F
    E --> F
    F --> G[Integration Tests]
    G --> H[E2E Tests]
    H --> I[Notify]
```

### Pipeline Jobs

| Job | Description |
|-----|-------------|
| Lint & Type Check | flake8, black, mypy |
| Unit Tests | pytest with coverage |
| Build Docker | Builds & pushes to GHCR |
| Integration Tests | PostgreSQL, MinIO connectivity |
| E2E Tests | Full stack, DAG imports, data generator |
| Security Scan | safety, bandit |

## Metabase Dashboards

The platform includes pre-configured dashboards for data visualization:

### Available Dashboards

| Dashboard | ID | Description | Source Tables |
|-----------|-----|-------------|---------------|
| Main Dashboard | 8 | Unified view with KPIs and charts | All gold tables |
| Executive Summary | 3 | High-level KPIs | gold.daily_sales |
| Sales Overview | 2 | Combined sales metrics | gold tables |
| Product Analytics | 4 | Product & category performance | gold.product_performance, gold.category_insights |
| Customer Analytics | 5 | Customer segments, LTV | gold.customer_analytics |
| Store Performance | 6 | Store metrics by location | gold.store_performance |
| Data Quality | 7 | Quality metrics, quarantine trends | quarantine.sales_failed |

### Accessing Dashboards

- **URL**: http://localhost:3000
- **Login**: agudeydaniel8@gmail.com / metabase123

### Quick Navigation

| Dashboard | Direct URL |
|-----------|------------|
| Main Dashboard | http://localhost:3000/dashboard/8-main-dashboard |
| Executive Summary | http://localhost:3000/dashboard/3-executive-summary |
| Product Analytics | http://localhost:3000/dashboard/4-product-analytics |
| Customer Analytics | http://localhost:3000/dashboard/5-customer-analytics |
| Store Performance | http://localhost:3000/dashboard/6-store-performance |
| Data Quality | http://localhost:3000/dashboard/7-data-quality |

### Data Quality Dashboard Features

The Data Quality Dashboard includes:

- **KPIs**: Quarantine Records, Quality Score (88.67%), Total Records Processed, Pending Remediation
- **Charts**: Error Type Distribution (bar), Failed Records Trend (line), Records by Source (pie)
- **Table**: Recent Failed Records with error details

### Connecting Metabase

1. Open http://localhost:3000
2. Add a new database:
   - **Database type**: PostgreSQL
   - **Host**: postgres
   - **Port**: 5432
   - **Database name**: airflow
   - **Username**: airflow
   - **Password**: airflow

### SQL Queries Documentation

All dashboard queries are documented in `docs/dashboard_queries.sql`:
- 40+ SQL queries categorized by dashboard
- Schema reference for all tables
- Additional insight queries for deeper analysis

### Creating Custom Dashboards

See `notes/METABASE_SETUP_GUIDE.md` for:
- How to create questions (step-by-step)
- Understanding collections
- Creating tabbed dashboards
- Adding filters
- Example queries

## Documentation

| File | Description |
|------|-------------|
| `README.md` | This file - overview and quick start |
| `docs/architecture.md` | Architecture diagrams and system design |
| `docs/dashboard_queries.sql` | All SQL queries for Metabase dashboards |
| `notes/METABASE_SETUP_GUIDE.md` | Guide to creating Metabase dashboards |

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "feat: add my feature"`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

## License

This project is for educational purposes as part of the Amalitech DEM012 CI/CD module.
