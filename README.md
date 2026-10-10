# RiskGraph

**Mapping Systemic Dependency Risk in the npm/JavaScript Open-Source
Ecosystem Using Big Data and Graph Analytics**

RiskGraph studies how npm package dependencies connect across the
JavaScript ecosystem and estimates which packages may have wider
structural impact if they become unavailable. It combines npm metadata,
GitHub Archive activity, distributed data processing, graph analytics,
heuristic risk scoring, and cascade simulation in an interactive
application.

> **Scope note:** RiskGraph is a research/educational prototype. Risk
> labels are heuristic systemic-dependency indicators, not vulnerability
> ratings or calibrated probabilities. Cascade results show potential
> structural reach under the graph assumptions; they do not guarantee
> that real applications will break.

## Features

-   Collect npm package metadata and dependency declarations.
-   Collect GitHub Archive activity for a defined UTC window.
-   Stream ingestion records through Kafka.
-   Store raw and processed data in RustFS object storage.
-   Process and transform data with Apache Spark.
-   Build a directed npm dependency graph and calculate graph metrics
    with NetworkX.
-   Generate heuristic package risk scores and categories.
-   Simulate potential dependency cascade reach from a selected package.
-   Explore outputs through FastAPI and a Streamlit dashboard.

## Architecture

``` text
npm Registry ───────┐
                    ├──> Ingestion ──> Kafka topics
GitHub Archive ─────┘                    │
                                         v
                              RustFS (S3-compatible storage)
                                         │
                                         v
                               Apache Spark processing
                                         │
                                         v
                             Dependency graph + metrics
                                         │
                              Risk scoring + cascade
                                         │
                              ┌──────────┴──────────┐
                              v                     v
                           FastAPI              Streamlit
```

## Technology stack

-   Python 3.11+
-   Docker Desktop and Docker Compose
-   Apache Kafka
-   RustFS (S3-compatible object storage)
-   Apache Spark 3.5.7
-   NetworkX 3.1
-   FastAPI
-   Streamlit and Plotly
-   Java 11 for the Spark environment

## Dataset and current results

Latest recorded results from this project run:

  Measure                                                               Result
  ---------------------------------------------- -----------------------------
  npm package records collected                                          4,000
  Raw GitHub Archive events                         Approximately 15.3 million
  Filtered GitHub events                           Approximately 16.8 thousand
  Repositories represented in filtered events                              679
  Dependency graph vertices                                              8,819
  Dependency graph edges                                                15,456
  Packages with matched GitHub observations                              1,550
  Packages without matched GitHub observations                           2,450
  HIGH risk                                                                116
  MEDIUM risk                                                            1,449
  LOW risk                                                               2,435

GitHub Archive data covers **2026-09-26 through 2026-10-02 (UTC)**. The
npm collection is a 4,000-package sample, not a complete crawl of the
registry. A package without matched activity in this seven-day window is
not necessarily inactive or unmaintained.

## Repository structure

``` text
RiskGraph/
├── analytics/
│   ├── risk_analysis.py
│   └── cascade_simulation.py
├── processing/
│   ├── prepare_datasets.py
│   └── build_graph.py
├── app/
│   ├── api/
│   │   └── main.py
│   └── dashboard/
│       └── Home.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

The ingestion scripts may be in an `ingestion/` directory, depending on
the current checkout. To see the exact files in your copy:

``` powershell
Get-ChildItem -Recurse -Filter *.py | Select-Object -ExpandProperty FullName
```

## Prerequisites

-   Docker Desktop installed and running, with Docker Compose support.
-   Git.
-   Python 3.11+ if running any scripts directly on the host.
-   Enough available memory for Spark and Docker. On a machine with 8 GB
    RAM, run processing stages one at a time and use conservative Spark
    memory settings.

For the Windows setup used during development, open PowerShell in the
repository root:

``` powershell
cd C:\Users\saima\.vscode\RiskGraph
```

When using a different computer, replace the path with the directory
where you cloned the repository.

## Configuration

Review `docker-compose.yml` and any `.env.example` file before starting.
Create a local `.env` only if the compose configuration requires it.
Never commit credentials or secrets.

Important service addresses:

-   RustFS inside Docker: `http://rustfs:9000`
-   RustFS from the host: `http://localhost:9000`
-   RustFS Console: `http://localhost:9001`
-   RustFS bucket: `riskgraph`
-   Spark master inside Docker: `spark://spark-master:7077`
-   Spark Master UI: `http://localhost:8081`

Kafka host/internal addresses and credentials must match the values in
the current `docker-compose.yml`.

## Run the application

### 1. Start the services

Run from the repository root:

``` powershell
docker compose up -d --build
```

Check that services are running:

``` powershell
docker compose ps
docker compose logs --tail 100
```

### 2. Open the web interfaces

Once the services are up, open these URLs in your browser:

  -----------------------------------------------------------------------------
  Service                 URL                           What it is for
  ----------------------- ----------------------------- -----------------------
  RiskGraph Dashboard     http://localhost:8501         Explore ecosystem
                                                        overview, package risk,
                                                        dependency graph, and
                                                        cascade simulations

  FastAPI Swagger         http://localhost:8000/docs    Inspect and test API
                                                        endpoints

  FastAPI ReDoc           http://localhost:8000/redoc   Alternative API
                                                        documentation

  RustFS Console          http://localhost:9001         Browse the
                                                        object-storage bucket
                                                        and data

  Spark Master UI         http://localhost:8081         Monitor Spark workers
                                                        and applications
  -----------------------------------------------------------------------------

These are **local URLs**, not public internet links. They work on the
computer running Docker. Anyone reproducing the project will access the
same services through `localhost` on their own computer.

### 3. Check the API

``` powershell
Invoke-RestMethod http://localhost:8000/overview
Invoke-RestMethod http://localhost:8000/package/express
```

Use http://localhost:8000/docs to verify the exact endpoints available
in the current version.

## Reproduce the data-processing pipeline

Run the stages in order. The raw input data must already exist in RustFS
before dataset preparation.

### 1. Prepare datasets

``` powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  processing/prepare_datasets.py
```

This stage reads raw data, filters relevant GitHub events, performs
required transformations and matching, and writes prepared outputs to
RustFS.

### 2. Build the dependency graph

``` powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-cores 2 `
  --executor-memory 1536M `
  --driver-memory 768M `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  processing/build_graph.py
```

This stage constructs package vertices and directed dependency edges
from the collected metadata.

### 3. Run risk analysis and cascade simulation

The exact command depends on how the current scripts define their entry
points and load their inputs. Check the script headers and arguments
first:

``` powershell
Get-Content .\analytics\risk_analysis.py -TotalCount 80
Get-Content .\analytics\cascade_simulation.py -TotalCount 80
```

Run each script using the invocation supported by that implementation.
The pipeline order is:

1.  Collect/ingest raw npm and GitHub data.
2.  Prepare and filter datasets.
3.  Build the dependency graph.
4.  Calculate graph metrics and risk scores.
5.  Run cascade simulations.
6.  Restart the API/dashboard if they need to reload generated outputs.

Do not rerun data collection just to reopen the dashboard. Inspect the
crawler/producer first: rerunning it may append duplicates or recollect
data.

## Kafka topics and ingestion

The project used these Kafka topics:

-   `npm-packages`
-   `github-events`

If the topic-creation script exists in your checkout, inspect and run it
from the repository root:

``` powershell
Get-ChildItem .\ingestion -File
python .\ingestion\create_topics.py
```

If the `ingestion/` directory or script is absent, use the recursive
Python-file listing above to find the actual current script names.
Ensure the project dependencies are installed before running host-side
Python scripts.

Optional host Python environment:

``` powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Data storage and graph convention

The RustFS bucket is named `riskgraph`. Raw data is stored under `raw/`,
while prepared datasets and analytics outputs are stored under
`processed/`; exact filenames depend on the scripts.

Graph direction is **A → B means package A depends on package B**:

-   **Out-degree:** number of dependencies used by package A.
-   **In-degree:** number of packages that depend on package A.

A package with high in-degree may have broad structural reach in the
dependency graph.

## Useful commands

``` powershell
docker compose ps
docker compose logs -f api
docker compose logs -f dashboard
docker compose logs -f kafka
docker compose logs -f rustfs
docker compose logs -f spark-master
docker compose restart api dashboard
docker compose stop
docker compose start
docker compose down
```

`docker compose down` removes containers and the network while normally
preserving named volumes. Avoid `docker compose down -v` unless you
intentionally want to delete persisted volumes and data.

## Troubleshooting

**A service is unavailable:** check `docker compose ps` and
`docker compose logs --tail 100 <service>`.

**Spark cannot find S3A classes:** the Spark job requires a Hadoop AWS
connector compatible with the Hadoop libraries in the Spark image. This
project used `org.apache.hadoop:hadoop-aws:3.3.4`; verify versions if
the error persists.

**Docker/Spark uses too much memory:** run one Spark job at a time, use
the memory settings above, and check Docker Desktop/WSL resource limits.

**API results are empty or stale:** verify processed objects exist in
RustFS, check `/overview`, inspect API logs, and confirm output
filenames/schema match what the API expects. Restart the API/dashboard
if they load results only at startup.

## Limitations

-   The npm sample is not the full registry.
-   GitHub activity covers only seven UTC days.
-   Package-to-repository matching can miss records.
-   Betweenness centrality is sampled.
-   Risk scores are project-defined heuristics, not calibrated
    probabilities or vulnerability scores.
-   Cascade simulation does not model lockfiles, exact version
    resolution, runtime behavior, or actual outage telemetry.

## Team contributions

Add the four team members and their actual responsibilities before
submitting the project.

## License

Add the license selected by your team or course before publishing the
repository.
