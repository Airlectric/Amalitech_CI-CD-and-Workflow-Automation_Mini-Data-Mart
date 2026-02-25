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
    end

    subgraph DW["PostgreSQL"]
        Metadata["Metadata<br/>(Ingestion State)"]
        Silver["Silver Layer<br/>(Cleaned Data)"]
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
    Silver --> Gold
    Gold --> Dash

    style Lake fill:#4A90D9
    style Airflow fill:#01778E
    style Bronze fill:#CD7F32
    style Silver fill:#C0C0C0
    style Gold fill:#FFD700
    style Metadata fill:#E74C3C
    style DW fill:#336791
    style Viz fill:#6B4C9A
```

---


## Data Flow Sequence Diagram

```mermaid
sequenceDiagram
    participant DG as Data Generator
    participant Bronze as MinIO (Bronze)
    participant AF as Airflow + DuckDB
    participant Meta as PostgreSQL (Metadata)
    participant Silver as PostgreSQL (Silver)
    participant Gold as PostgreSQL (Gold)
    participant MB as Metabase

    DG->>Bronze: 1. Upload Parquet
    AF->>Bronze: 2. Read with DuckDB
    AF->>Bronze: 3. Schema Validation
    AF->>Meta: 4. Update Metadata (NEW)
    AF->>Meta: 5. Mark PROCESSED
    AF->>Silver: 6. Insert Cleaned Data
    AF->>Gold: 7. Aggregate Metrics
    MB->>Gold: 8. Query Data
    MB->>MB: 9. Visualize
```



---

## Services Architecture

```mermaid
%%{init: {'theme': 'base'}}}%%
flowchart LR

    subgraph Services["DOCKER COMPOSE"]
        direction LR
        AF[Airflow<br/>+ DuckDB]
        DB[(PostgreSQL<br/>Silver, Gold, Metadata)]
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

## Testing & Validation

```mermaid
%%{init: {'theme': 'base'}}}%%
flowchart LR

    subgraph Testing["TESTING & VALIDATION"]
        direction TB
        Unit[Unit Tests<br/>pytest]
        Schema[Schema Tests<br/>Great Expectations]
        Drift[Drift Tests<br/>Evidently]
        E2E[E2E Tests<br/>CI/CD]
    end

    subgraph Data["DATA PIPELINE"]
        direction LR
        Bronze
        Silver
        Gold
    end

    Unit -->|Test Code| Data
    Schema -->|Validate| Bronze
    Schema -->|Validate| Silver
    Drift -->|Monitor| Silver
    E2E -->|Validate Flow| Data
```

---

## CI/CD Pipeline

```mermaid
%%{init: {'theme': 'base'}}}%%
flowchart LR

    subgraph CI["CI - ON COMMIT"]
        direction TB
        Build[Build Docker]
        UnitTest[Unit Tests]
        SchemaTest[Schema Tests]
    end

    subgraph CD["CD - DEPLOY"]
        direction TB
        Deploy[Deploy to Test]
        E2E[E2E Validation]
        Validate[Data Flow Check]
    end

    CI --> Build
    CI --> UnitTest
    CI --> SchemaTest
    CD --> Deploy
    CD --> E2E
    CD --> Validate
```

---

## Complete System Overview

```mermaid
%%{init: {'theme': 'base'}}}%%
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
        DB[PostgreSQL<br/>Silver, Gold, Metadata]
        MB[Metabase]
    end

    DG -->|1. Generate| Lake
    Lake -->|2. Process| AF
    AF -->|3. Store| DB
    DB -->|4. Visualize| MB
    
    Tests -->|Validate| DG
    CI -->|Deploy| Runtime
```
