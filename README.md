# RiskGraph — Big Data Analytics

A Big Data Analytics project for mapping systemic dependency risk in the npm/JavaScript open-source ecosystem using Kafka, RustFS, Apache Spark, and graph analytics.

RiskGraph aims to analyze package dependencies, identify critical packages and maintainers, measure ecosystem health, and simulate cascading failures caused by dependency disruptions.

## Architecture

- **Data Sources:** npm Registry API, GitHub Events API (with planned GH Archive integration)
- **Ingestion:** Python producers
- **Streaming:** Apache Kafka
- **Object Storage:** RustFS (S3-compatible object storage)
- **Data Processing:** Apache Spark / PySpark
- **Graph Analytics:** NetworkX (prototype), with planned GraphFrames / GraphX
- **Backend:** FastAPI
- **Dashboard:** Streamlit
- **Infrastructure:** Docker Compose

## Requirements

- Docker Desktop with Docker Compose v2
- Python 3.11+
- VS Code (recommended)
- Git (recommended)

## Project Structure

```text
RiskGraph/
├── analytics/
│   └── build_graph.py
├── api/
├── dashboard/
├── ingestion/
│   ├── create_topics.py
│   ├── npm_producer.py
│   ├── github_producer.py
│   └── consume_sample.py
├── storage/
│   └── rustfs_client.py
├── data/
├── docker-compose.yml
├── requirements.txt
├── .env
├── .env.example
├── .gitignore
└── README.md
```

## Environment Setup

Clone the repository and navigate to the project directory.

```powershell
git clone <your-repository-url>
cd RiskGraph
```

Create a Python virtual environment:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the required dependencies:

```powershell
python -m pip install -r requirements.txt
```

If PowerShell blocks virtual environment activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Environment Variables

Create a `.env` file in the project root for configuration such as GitHub authentication and Kafka settings.

```env
GITHUB_TOKEN=your_github_personal_access_token
GITHUB_REPO=nodejs/node

KAFKA_BOOTSTRAP_SERVERS=localhost:29092
GITHUB_EVENTS_TOPIC=github-events
NPM_PACKAGES_TOPIC=npm-packages
```

Use a GitHub Personal Access Token to help avoid unauthenticated API rate limits. Keep the token private and never commit `.env` to Git.

Use `.env.example` to document the required variable names without including real credentials.

## Start Docker Services

Make sure Docker Desktop is running. From the project root, execute:

```powershell
docker compose up -d --build
```

Check the service status:

```powershell
docker compose ps
```

The project uses Docker Compose to run Kafka, RustFS, Spark Master, Spark Worker, FastAPI, and Streamlit.

### Service URLs

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| FastAPI Documentation | http://localhost:8000/docs |
| RustFS Console | http://localhost:9001 |
| RustFS S3 API | http://localhost:9000 |
| Spark Master UI | http://localhost:8081 |
| Kafka (host access) | `localhost:29092` |
| Kafka (inside Compose) | `kafka:9092` |

**Local RustFS credentials**

- Username / Access key: `riskgraph`
- Password / Secret key: `riskgraph_local_password`

These are local development credentials only. Change them before using the setup in a shared or production environment.

## Kafka Setup and Data Ingestion

### 1. Create Kafka Topics

Activate your Python virtual environment and run:

```powershell
python ingestion/create_topics.py
```

The current topics are:

- `npm-packages`
- `github-events`

Verify the topics from Docker:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### 2. Ingest npm Package Metadata

The npm producer fetches package metadata directly from the npm Registry API for a small initial set of packages.

Run:

```powershell
python ingestion/npm_producer.py
```

The initial sample includes:

- express
- react
- lodash
- debug
- chalk

The producer publishes package names, versions, descriptions, dependencies, development dependencies, maintainers, and collection timestamps to the `npm-packages` topic.

### 3. Ingest GitHub Events

Run:

```powershell
python ingestion/github_producer.py
```

The current GitHub producer uses the public GitHub Events API for a small sample feed. The repository can be configured through `.env`.

The producer publishes event information, including event type, repository, actor, timestamp, and payload, to the `github-events` topic.

This is an initial sample ingestion method. GH Archive hourly JSON ingestion is planned for broader historical and ecosystem coverage.

### 4. Verify Kafka Messages

Check the npm topic offsets:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic npm-packages
```

Consume npm messages:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic npm-packages --from-beginning --timeout-ms 10000
```

Consume GitHub events:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic github-events --from-beginning --timeout-ms 10000
```

When running Python ingestion scripts directly from Windows, use `localhost:29092` as the Kafka bootstrap server. When running services inside the Docker Compose network, use `kafka:9092`.

## RustFS Object Storage

RiskGraph uses RustFS as its S3-compatible object storage layer for the data lake.

The Python MinIO SDK is used to communicate with RustFS through its S3-compatible API.

The connection is configured in:

```text
storage/rustfs_client.py
```

For Python scripts running directly on the host, use:

```python
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="riskgraph",
    secret_key="riskgraph_local_password",
    secure=False
)

bucket = "riskgraph"

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
```

For code running inside a Docker Compose container, use `rustfs:9000` instead of `localhost:9000`, assuming the RustFS service is named `rustfs`.

Test the connection from Windows PowerShell:

```powershell
python -c "from storage.rustfs_client import client; print(client.list_buckets())"
```

The `riskgraph` bucket has been created and its existence verified.

**Current status:** RustFS connectivity and bucket initialization are verified. A Kafka-to-RustFS consumer that persists the ingested messages is the next implementation stage.

## Spark Processing

RiskGraph uses PySpark to process ingested data and prepare package dependency graphs.

The starter graph extraction script is:

```text
analytics/build_graph.py
```

Run it from the host virtual environment:

```powershell
python analytics/build_graph.py
```

The initial prototype consumes a small Kafka sample and produces vertex and edge datasets in the local `data/` directory.

The script is a starting point for graph extraction. Distributed Spark processing, Iceberg integration, and scalable graph algorithms remain planned implementation stages.

## Graph Analytics

The project is intended to analyze package dependencies and ecosystem risk through:

- **Dependency Graph:** Represent packages as vertices and dependencies as directed edges.
- **PageRank:** Identify packages with structural importance in the dependency network.
- **Centrality:** Measure package connectivity and potential impact.
- **Community Detection:** Identify groups of closely connected packages.
- **Maintenance Risk:** Incorporate package and maintainer activity signals.
- **Cascading Failure Simulation:** Model potential propagation of package disruptions through dependent packages.

These analytics will be implemented incrementally after the data storage and processing pipelines are established.

## API and Dashboard

### FastAPI

The backend service is available at:

http://localhost:8000/docs

FastAPI will expose processed graph analytics and risk information through API endpoints as implementation progresses.

### Streamlit

The dashboard is available at:

http://localhost:8501

Streamlit will be used to visualize package dependencies, graph metrics, ecosystem health indicators, and cascading failure scenarios as the analytics are developed.

## Current Implementation Status

| Component | Status |
|---|---|
| Docker Compose infrastructure | Running |
| Kafka broker and topics | Verified |
| GitHub event producer | Published sample events |
| npm package producer | Published sample packages |
| Kafka consumer verification | Verified for npm |
| RustFS connection | Verified |
| `riskgraph` bucket | Created |
| Kafka-to-RustFS persistence | Pending |
| Spark data processing | Starter prototype |
| Distributed graph analytics | Planned |
| Maintenance-risk model | Planned |
| Cascading-failure simulation | Planned |
| Full dashboard integration | Planned |

## Suggested Implementation Roadmap

1. Persist raw Kafka messages to RustFS in JSONL format.
2. Expand ingestion to GH Archive and a larger set of npm packages.
3. Read stored data from RustFS using PySpark.
4. Clean, normalize, and transform package metadata and GitHub activity.
5. Build the package dependency graph with vertices and directed edges.
6. Implement PageRank, centrality, and community detection.
7. Develop maintenance-risk indicators and cascading-failure simulations.
8. Expose processed results through FastAPI.
9. Integrate graph analytics and visualizations into Streamlit.
10. Evaluate the system with larger datasets and document findings.

## Stop and Reset Services

Stop the containers while preserving their volumes:

```powershell
docker compose down
```

To stop the containers and remove their associated Compose volumes:

```powershell
docker compose down -v
```

**Warning:** `docker compose down -v` is destructive and may permanently delete local Kafka and RustFS data stored in Compose-managed volumes.

## Notes

- The current ingestion scripts use small samples for pipeline testing, not complete ecosystem coverage.
- Host-side scripts use `localhost:29092` for Kafka and `localhost:9000` for RustFS.
- Containerized services should use their Compose service names and internal ports.
- Do not commit API tokens, passwords, or other secrets to version control.
- The current starter is an incremental prototype; distributed analytics and full data lake integration are ongoing development tasks.

## License

Add the project's chosen license here before public distribution.
