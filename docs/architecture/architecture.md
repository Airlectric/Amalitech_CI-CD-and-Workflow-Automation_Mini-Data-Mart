# Mini Data Platform Architecture

## Data Flow Architecture

```mermaid
flowchart TB
    subgraph Source["Data Source"]
        DG["Data Generator"]
    end

    subgraph Lake["MinIO (Data Lake)"]
        Bronze["Bronze Layer<br/>(Parquet Files)"]
    end

    subgraph Airflow["Apache Airflow"]
        DuckDB["DuckDB<br/>(Schema-on-Read)"]
        Pipeline["ETL Pipelines"]
        Quality["Data Quality<br/>(SQL-based)"]
        Remediation["Remediation<br/>(Auto-fix)"]
    end

    subgraph DW["PostgreSQL"]
        Metadata["Metadata<br/>(Ingestion State)"]
        Quarantine["Quarantine<br/>(Failed Records)"]
        Silver["Silver Layer<br/>(sales, customers, products)"]
        Gold["Gold Layer<br/>(Aggregated)"]
    end

    subgraph Viz["Metabase"]
        Dash["Dashboards"]
    end

    DG -->|Upload Parquet| Bronze
    Bronze -->|Schema Validation| DuckDB
    DuckDB -->|Clean & Transform| Pipeline
    Pipeline --> Metadata
    Pipeline --> Silver
    Pipeline --> Quarantine
    Silver --> Quality
    Quality -->|PDF Report| Email
    Quarantine -->|Auto-fix| Remediation
    Remediation -->|Replay| Silver
    Silver --> Gold
    Gold --> Dash

    style Lake fill:#4A90D9
    style Airflow fill:#01778E
    style Bronze fill:#CD7F32
    style Silver fill:#C0C0C0
    style Gold fill:#FFD700
    style Quarantine fill:#E74C3C
    style Metadata fill:#9B59B6
    style DW fill:#336791
    style Viz fill:#6B4C9A
    style Quality fill:#27AE60
    style Remediation fill:#E67E22
```

---

## Updated Data Flow Sequence Diagram

```mermaid
sequenceDiagram
    participant DG as Data Generator
    participant Bronze as MinIO (Bronze)
    participant AF as Airflow + DuckDB
    participant Quarantine as PostgreSQL (Quarantine)
    participant Silver as PostgreSQL (Silver)
    participant Quality as Data Quality
    participant Gold as PostgreSQL (Gold)
    participant Email as Email Alerts
    participant MB as Metabase

    Note over DG, Bronze: 1. Generate & Upload
    DG->>Bronze: Upload Parquet Files

    Note over Bronze, Silver: 2. Ingest & Validate
    AF->>Bronze: Read with DuckDB
    AF->>Bronze: Schema Validation
    AF->>Silver: Insert Clean Data
    AF->>Quarantine: Insert Failed Records

    Note over Silver, Silver: 3. Populate Dimensions
    AF->>Silver: Extract customers from sales
    AF->>Silver: Extract products from sales

    Note over Silver, Quarantine: 4. Quality Check
    AF->>Silver: Profile all tables
    AF->>Quarantine: Validate patterns
    AF->>Quality: Detect drift
    AF->>Quality: Generate PDF Report
    Quality->>Email: Send email with PDF

    Note over Quarantine, Silver: 5. Remediation (if needed)
    AF->>Quarantine: Get failed records
    AF->>Quarantine: Validate & Fix
    AF->>Quarantine: Mark as replayed
    AF->>Silver: Replay fixed records

    Note over Silver, Gold: 6. Aggregate
    AF->>Silver: Build dimensions
    AF->>Gold: Daily aggregations

    Note over Gold, MB: 7. Visualize
    MB->>Gold: Query data
    MB->>MB: Visualize
```

---

## DAG Pipeline Flow

```mermaid
flowchart LR
    subgraph "Scheduled/Manual"
        A[generate_sample_data]
    end
    
    subgraph "ingest_bronze_to_silver"
        B[discover_and_ingest]
        C[populate_customers]
        D[populate_products]
    end
    
    subgraph "data_quality_checks"
        E[validate_quarantine]
        F[profile_silver]
        G[detect_drift]
        H[generate_data_docs]
        I[send_alerts]
    end
    
    subgraph "silver_to_gold"
        J[populate_dimensions]
        K[aggregate_gold]
    end
    
    subgraph "remediation_workflow"
        L[get_quarantined]
        M[validate_records]
        N[fix_and_replay]
        O[notify]
    end
    
    A -->|Triggers| B
    B --> C
    B --> D
    C --> E
    D --> E
    B --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J
    J --> K
    L --> M
    M --> N
    N --> O
```

---

## Services Architecture

```mermaid
%%{init: {'theme': 'base'}}}
flowchart LR

    subgraph Services["DOCKER COMPOSE"]
        direction LR
        AF[Airflow<br/>+ DuckDB]
        DB[(PostgreSQL<br/>Silver, Gold, Quarantine, Metadata)]
        Redis[(Redis)]
        MinIO[MinIO<br/>Bronze Layer]
        MB[Metabase]
    end

    AF --> DB
    AF --> Redis
    AF <--> MinIO
    MB --> DB
```

---

## Data Quality Architecture

```mermaid
flowchart TB
    subgraph Input["Quality Inputs"]
        Q[quarantine.sales_failed]
        S[silver.sales]
        C[silver.customers]
        P[silver.products]
    end

    subgraph Quality["Data Quality Pipeline"]
        V[validate_quarantine]
        Pr[profile_silver]
        D[detect_drift]
        G[generate_data_docs]
    end

    subgraph Output["Quality Outputs"]
        PDF[PDF Report]
        Email[Email Alert]
    end

    Q --> V
    S --> Pr
    C --> Pr
    P --> Pr
    V --> D
    Pr --> D
    D --> G
    G --> PDF
    PDF --> Email

    style Quality fill:#27AE60
    style Output fill:#3498DB
```

---

## Remediation Workflow

```mermaid
flowchart TD
    Start[Quarantined Records] --> Get[Get Failed Records]
    Get --> Validate[Validate Records]
    
    Validate --> Valid{Valid?}
    Valid -->|Yes| Fix[Fix & Transform]
    Valid -->|No| Reject[Mark as Rejected]
    
    Fix --> Insert[Insert to Silver]
    Insert --> Update[Update Quarantine Status]
    Reject --> Update
    
    Update --> Notify[Send Notification]
    
    style Valid fill:#27AE60
    style Fix fill:#3498DB
    style Reject fill:#E74C3C
```

---

## Testing & Validation

```mermaid
%%{init: {'theme': 'base'}}}
flowchart LR

    subgraph Testing["TESTING & VALIDATION"]
        direction TB
        Unit[Unit Tests<br/>pytest]
        Integration[Integration Tests<br/>Docker]
        E2E[E2E Tests<br/>Full Pipeline]
        Security[Security Scan<br/>safety, bandit]
    end

    subgraph Data["DATA PIPELINE"]
        direction LR
        Bronze
        Silver
        Gold
    end

    Unit -->|Test Code| Data
    Integration -->|Test Connect| Data
    E2E -->|Validate Flow| Data
    Security -->|Scan Code| Dev
```

---

## CI/CD Pipeline

```mermaid
%%{init: {'theme': 'base'}}}
flowchart LR

    subgraph CI["CI - ON PUSH/PR"]
        direction TB
        Lint[Lint & Format<br/>flake8, black, mypy]
        Unit[Unit Tests<br/>pytest]
        Build[Build Docker<br/>GHCR]
    end

    subgraph CD["CD - DEPLOY"]
        direction TB
        Integration[Integration Tests<br/>PostgreSQL, MinIO]
        E2E[E2E Tests<br/>Full Stack]
        Notify[GitHub Summary]
    end

    Push[Push/PR] --> CI
    CI --> Lint
    CI --> Unit
    CI --> Build
    Build --> CD
    CD --> Integration
    CD --> E2E
    E2E --> Notify

    style CI fill:#3498DB
    style CD fill:#27AE60
    style Push fill:#E74C3C
```

---

## Complete System Overview

```mermaid
%%{init: {'theme': 'base'}}}
flowchart TB

    subgraph Dev["DEVELOPMENT"]
        direction TB
        DG[Data Generator]
        Tests[Tests & Validation]
        CI[GitHub Actions]
    end

    subgraph Runtime["RUNTIME ENVIRONMENT"]
        direction LR
        Lake[MinIO Bronze]
        AF[Airflow + DuckDB]
        Quality[Data Quality<br/>SQL-based]
        DB[PostgreSQL<br/>Silver, Gold, Quarantine]
        MB[Metabase]
    end

    DG -->|1. Generate| Lake
    Lake -->|2. Process| AF
    AF -->|3. Validate| Quality
    Quality -->|4. Report| Email
    AF -->|5. Store| DB
    DB -->|6. Visualize| MB
    
    Tests -->|Validate| DG
    CI -->|Deploy| Runtime
```

---

## Schema Summary

### Bronze Layer (MinIO)
- **Bucket**: `bronze`
- **Format**: Parquet files
- **Structure**: Partitioned by `ingest_date`

### Silver Layer (PostgreSQL)
- **Schema**: `silver`
- **Tables**: 
  - `sales` - Transaction fact table
  - `customers` - Customer dimension (populated during ingest)
  - `products` - Product dimension (populated during ingest)

### Quarantine Layer (PostgreSQL)
- **Schema**: `quarantine`
- **Tables**:
  - `sales_failed` - Failed records awaiting remediation

### Gold Layer (PostgreSQL)
- **Schema**: `gold`
- **Tables**: Aggregated analytics (daily_sales, product_performance, etc.)

### Metadata Layer (PostgreSQL)
- **Schema**: `metadata`
- **Tables**:
  - `ingestion_metadata` - Track processed files
  - `audit.ingestion_runs` - Audit trail

---

## Metabase Visualization Layer

### Dashboard Architecture

```mermaid
flowchart TB
    PG[(PostgreSQL)]
    MB[Metabase]
    Q[Questions]
    D[Dashboards]
    V[Views/Models]
    
    PG -->|Query Data| MB
    MB --> V
    V --> Q
    Q --> D
```

### Pre-configured Dashboards

| Dashboard | Metrics | Charts |
|-----------|---------|--------|
| Sales Overview | Daily revenue, transactions | Line, Bar |
| Product Performance | Revenue by product, category | Pie, Table |
| Customer Analytics | Segments, LTV | Funnel, Scatter |
| Store Performance | Revenue by location | Map, Bar |
| Data Quality | Failed records, trends | Trend, Gauge |

### Database Connection Setup

1. Navigate to Metabase Admin > Databases
2. Add PostgreSQL database:
   ```
   Host: postgres
   Port: 5432
   Database: airflow
   Username: airflow
   Password: airflow
   ```

### Recommended Visualizations

- **Time Series**: Daily sales trends (line chart)
- **Categorical**: Revenue by product category (pie/bar)
- **Ranking**: Top 10 products/customers (table with ranking)
- **Geographic**: Sales by store location (map)
