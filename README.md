# Mini Data Platform

> A containerized data platform using Docker Compose that collects, processes, stores, and visualizes data — with full CI/CD automation via GitHub Actions.

<!-- TODO: Add CI/CD status badges here -->

## Architecture

<!-- TODO: Add architecture diagram in Phase 7 -->

```
CSV Generator → MinIO (Storage) → Airflow (ETL) → PostgreSQL (Database) → Metabase (Dashboards)
```

## Tech Stack

| Component       | Technology       | Port      | Purpose                     |
|-----------------|------------------|-----------|-----------------------------|
| Database        | PostgreSQL 16    | 5432      | Structured data storage     |
| Object Storage  | MinIO            | 9000/9001 | S3-compatible file storage  |
| Orchestration   | Apache Airflow   | 8080      | ETL pipeline scheduling     |
| Visualization   | Metabase         | 3000      | BI dashboards & reporting   |
| CI/CD           | GitHub Actions   | —         | Automated pipelines         |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose v2+
- [Git](https://git-scm.com/)
- 8GB+ RAM recommended

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd <repo-name>

# 2. Set up environment variables
cp .env.example .env

# 3. Start all services
docker compose up -d

# 4. Wait for services to initialize (~2 minutes)
docker compose ps

# 5. Access the services (see table below)
```

## Services & Ports

| Service         | URL                          | Default Credentials         |
|-----------------|------------------------------|-----------------------------|
| Airflow UI      | http://localhost:8080        | admin / admin               |
| Metabase        | http://localhost:3000        | Set up on first visit       |
| MinIO Console   | http://localhost:9001        | minioadmin / changeme123    |
| PostgreSQL      | localhost:5432               | dataplatform / changeme     |

## Project Structure

```
.
├── .github/workflows/       # CI/CD pipeline definitions
├── dags/                    # Airflow DAG definitions
├── data/sample/             # Generated sample CSV files
├── docker/
│   ├── airflow/             # Custom Airflow Dockerfile
│   └── data-generator/      # Data generator Dockerfile
├── scripts/                 # Utility & bootstrap scripts
├── dashboards/              # Metabase export configs
├── docs/
│   ├── architecture/        # Architecture diagrams
│   └── screenshots/         # Dashboard screenshots
├── docker-compose.yml       # All services definition
├── .env.example             # Environment variable template
├── .gitignore
└── README.md
```

## Running the Pipeline

<!-- TODO: Add step-by-step pipeline instructions in Phase 4 -->

```bash
# Generate sample data
docker compose run --rm data-generator

# Trigger the ETL pipeline (or wait for scheduled run)
# Open Airflow UI → DAGs → sales_etl_pipeline → Trigger

# View dashboards
# Open Metabase at http://localhost:3000
```

## Dashboard Screenshots

<!-- TODO: Add screenshots in Phase 5 -->

## CI/CD Pipeline

<!-- TODO: Add CI/CD workflow descriptions in Phase 6 -->

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "feat: add my feature"`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

## License

This project is for educational purposes as part of the Amalitech DEM012 CI/CD module.
