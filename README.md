# RiskGraph — Setup Guide

This repo uses Docker Compose so all 4 team members run the **exact same environment** —
no "works on my machine" problems.

## Prerequisites (everyone installs this once)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git installed
- At least 8GB RAM free (Spark + Kafka + MinIO together are not lightweight)

## First-time setup (every team member does this)

```bash
# 1. Clone the repo
git clone <repo-url>
cd riskgraph

# 2. Start the whole stack (Kafka, MinIO, Spark)
docker compose up -d

# 3. Check everything is running
docker compose ps
```

You should see 6 containers running: zookeeper, kafka, kafka-ui, minio, spark-master, spark-worker.

## Where to check things are working

| Service         | URL                          | Login                          |
|------------------|-------------------------------|---------------------------------|
| Kafka UI         | http://localhost:8085         | none                             |
| MinIO Console    | http://localhost:9001         | minioadmin / minioadmin          |
| Spark Master UI  | http://localhost:8080         | none                             |

If you can open all three in a browser, your environment is correctly set up.

## Stopping everything
```bash
docker compose down
```
Add `-v` if you also want to wipe stored data: `docker compose down -v`

## Folder structure
```
riskgraph/
├── docker-compose.yml
├── README.md
├── .gitignore
├── ingestion/        <- Member 1: Kafka producers/consumers, GH Archive parsing
├── storage/          <- Member 2: MinIO/Iceberg setup, npm dependency graph build
├── analytics/        <- Member 3: Spark/GraphFrames, centrality, bus-factor, cascading failure sim
└── dashboard/         <- Member 4: FastAPI backend + Streamlit frontend
```

## Git workflow
- Don't push directly to `main`.
- Create a branch per feature: `git checkout -b member1-ingestion`
- Push your branch, open a Pull Request on GitHub, get at least one teammate to glance at it before merging.
- Pull `main` regularly (`git pull origin main`) so you don't drift too far from everyone else's work.