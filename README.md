# Mini Data Platform

> A production-grade containerized data platform using Docker Compose that implements medallion architecture (Bronze Silver → Gold) with full → CI/CD automation.

## Architecture

See [docs/architecture.md](docs/architecture/architecture.md) for detailed diagrams.

```
Data Generator → MinIO (Bronze) → Airflow + DuckDB → PostgreSQL (Silver/Gold) → Metabase
```

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
| Airflow UI      | http://localhost:8080        | admin / airflow (set via .env, FABAuthManager) |
| Metabase        | http://localhost:3000        | Set up on first visit  |
| MinIO Console   | http://localhost:9003        | minio / minio123       |
| PostgreSQL      | localhost:5433               | airflow / airflow      |

## Project Structure

```
.
├── .github/workflows/       # CI/CD pipeline definitions
├── dags/                   # Airflow DAG definitions
├── data/                   # Data storage (local)
├── docs/                   # Documentation
│   └── architecture.md     # Architecture diagrams
├── scripts/                # Utility scripts
│   ├── data_generator/     # Parquet data generator
│   ├── init-db.sh         # Database initialization
│   └── init-minio.sh      # MinIO bucket setup
├── sql/                    # SQL schemas
│   ├── silver/            # Silver layer tables
│   └── gold/              # Gold layer tables
├── dashboards/             # Metabase configurations
├── docker-compose.yml      # All services definition
├── Dockerfile             # Airflow custom image
├── requirements.txt        # Python dependencies
├── .env                   # Environment variables
└── README.md
```

## Data Flow

1. **Generate** - Data generator creates Parquet files
2. **Ingest** - Files uploaded to MinIO (Bronze layer)
3. **Process** - Airflow + DuckDB validates and transforms
4. **Store** - Cleaned data in Silver, aggregations in Gold
5. **Visualize** - Metabase queries Gold tables

## Running the Pipeline

```bash
# Generate and upload data to MinIO
docker compose exec airflow-worker python scripts/data_generator/generator.py

# Trigger DAGs via Airflow UI at http://localhost:8080
```

## CI/CD Pipeline

The project includes GitHub Actions workflows for:
- Building Docker images on every commit
- Running unit and schema tests
- Deploying to test environment
- Validating data flow through all components

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "feat: add my feature"`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

## License

This project is for educational purposes as part of the Amalitech DEM012 CI/CD module.
