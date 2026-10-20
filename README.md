# RiskGraph
### Mapping Systemic Dependency Risk in the npm/JavaScript Open-Source Ecosystem Using Big Data and Graph Analytics

RiskGraph is a big-data analytics project that investigates how dependencies between npm packages connect across the JavaScript ecosystem and identifies packages that may have a wider structural impact if they become unavailable.

Modern JavaScript applications depend on large networks of third-party packages. A problem affecting one widely used package can potentially propagate to other packages and applications through these dependency relationships. RiskGraph models this ecosystem as a directed graph, calculates structural metrics, and uses heuristic risk scoring and cascade simulation to explore potential dependency-related impacts.

The project combines data ingestion, distributed processing, object storage, graph analytics, and interactive visualization in an end-to-end data pipeline.

> **Research scope:** RiskGraph is an educational and research prototype. Its risk categories are heuristic indicators of structural dependency risk, not security vulnerability ratings or calibrated probabilities. Cascade simulations estimate potential reach under the project's graph assumptions; they do not establish that real applications will fail.

---

## 1. Project Objectives

RiskGraph aims to:

- Collect npm package metadata and dependency declarations.
- Collect GitHub Archive activity for a defined UTC time window.
- Use Apache Kafka to stream ingestion records through separate topics.
- Store raw and processed datasets in RustFS object storage.
- Use Apache Spark to clean, transform, filter, and prepare large datasets.
- Build a directed dependency graph using NetworkX.
- Calculate graph metrics to identify packages with potentially significant structural influence.
- Generate heuristic package risk scores and classify packages into risk categories.
- Simulate potential dependency cascade reach from a selected package.
- Expose analytical results through a FastAPI backend and an interactive Streamlit dashboard.

## 2. System Architecture

The architecture separates data collection, storage, processing, analysis, and presentation into distinct stages.

```text
                 DATA SOURCES
          ┌────────────┬──────────────┐
          │            │              │
          ▼            ▼              │
     npm Registry  GitHub Archive     │
          │            │              │
          └──────┬─────┘              │
                 ▼
          DATA INGESTION
                 │
                 ▼
           APACHE KAFKA
          ┌──────┴──────┐
          │             │
    npm-packages   github-events
          │             │
          └──────┬──────┘
                 ▼
         RUSTFS OBJECT STORAGE
            (S3-compatible)
                 │
                 ▼
        APACHE SPARK PROCESSING
     Cleaning, filtering, matching
        and dataset preparation
                 │
                 ▼
       DEPENDENCY GRAPH BUILDING
          NetworkX + graph data
                 │
                 ▼
          GRAPH ANALYTICS
       ┌─────────┴─────────┐
       │                   │
       ▼                   ▼
  Risk Scoring       Cascade Simulation
       │                   │
       └─────────┬─────────┘
                 ▼
          FASTAPI BACKEND
                 │
                 ▼
        STREAMLIT DASHBOARD
```

### End-to-end workflow

1. **Data collection:** Retrieve npm package metadata and GitHub Archive events.
2. **Streaming ingestion:** Publish incoming records to the appropriate Kafka topics.
3. **Object storage:** Persist raw data and make it available for downstream processing through RustFS.
4. **Big-data processing:** Use Spark to filter events, transform records, match package-related observations, and prepare structured datasets.
5. **Graph construction:** Represent packages as vertices and their declared dependencies as directed edges.
6. **Graph analytics:** Calculate structural metrics, generate heuristic risk scores, and simulate potential dependency cascades.
7. **Serving and visualization:** Expose results through FastAPI and explore them using Streamlit and Plotly.

---

## 3. Technology Stack

| Technology | Role in RiskGraph |
|---|---|
| Python 3.11+ | Data ingestion, analytics, API, and application logic |
| Apache Kafka | Streaming ingestion through named topics |
| RustFS | S3-compatible storage for raw and processed data |
| Apache Spark 3.5.7 | Distributed dataset processing and transformation |
| NetworkX 3.1 | Directed graph construction and graph analytics |
| FastAPI | REST API for accessing analytical results |
| Streamlit | Interactive dashboard and exploration interface |
| Plotly | Interactive visualizations |
| Docker Desktop | Local containerized development environment |
| Docker Compose | Service orchestration |
| Java 11 | Java runtime for the Spark environment |

---

## 4. Dataset and Recorded Results

The following figures represent the latest recorded results from this project run. They describe the collected sample and the outputs generated by the pipeline, rather than the entire npm ecosystem.

### Dataset overview

| Measure | Recorded result |
|---|---:|
| npm package records collected | 4,000 |
| Raw GitHub Archive events | Approximately 15.3 million |
| Filtered GitHub events | Approximately 16,800 |
| Repositories represented in filtered events | 679 |
| Dependency graph vertices | 8,819 |
| Dependency graph edges | 15,456 |
| Packages with matched GitHub observations | 1,550 |
| Packages without matched GitHub observations | 2,450 |

**GitHub Archive collection window:** September 26–October 2, 2026, UTC.

### Package risk distribution

| Risk category | Packages |
|---|---:|
| HIGH | 116 |
| MEDIUM | 1,449 |
| LOW | 2,435 |
| **Total** | **4,000** |

<text>These categories describe the package sample used for risk classification. The dependency graph contains more vertices because it can also include packages referenced by the collected packages but not included in the original 4,000-package sample.</text>

### Interpreting the results

- **Graph size:** The graph contains 8,819 vertices and 15,456 directed dependency edges, illustrating how a relatively small package sample can connect to a larger dependency network.
- **Matched activity:** GitHub observations were matched to 1,550 of the 4,000 collected packages.
- **Unmatched activity:** The absence of a match during the selected seven-day window does not establish that a package is inactive, abandoned, or unmaintained.
- **High-risk classification:** The 116 HIGH-risk packages are those classified by the project's scoring methodology. This result does not mean that 116 packages are vulnerable or likely to fail.
- **Cascade reach:** Simulation results illustrate potential structural propagation through the modeled graph, subject to the graph direction and simulation assumptions.

---

## 5. Data Collection and Ingestion

RiskGraph combines two primary data sources.

### npm Registry

The npm Registry supplies package metadata and dependency declarations. These records provide the information required to identify packages and construct dependency relationships.

The project collected a sample of 4,000 package records rather than crawling the entire registry.

### GitHub Archive

GitHub Archive provides event records describing public GitHub activity. The project uses a defined seven-day UTC window and filters the collected events to obtain a smaller dataset for analysis.

GitHub activity is used as an observational signal. It should not be treated as a complete measure of package maintenance, quality, popularity, or reliability.

### Kafka topics

The project uses two Kafka topics:

| Topic | Purpose |
|---|---|
| `npm-packages` | Carries npm package records |
| `github-events` | Carries GitHub Archive event records |

Kafka acts as the streaming layer between data collection and downstream data handling. Producers publish records to topics, allowing consumers to read and process the streams independently.

The two topics separate different record types and make the ingestion workflow easier to organize.

### RustFS object storage

RustFS provides S3-compatible object storage for the pipeline.

- `raw/` stores raw collected data.
- `processed/` stores prepared datasets and analytical outputs.
- `riskgraph` is the configured bucket name.

Keeping raw and processed data separate supports reproducibility and allows processing stages to work with persisted inputs rather than requiring data collection to run every time.

---

## 6. Big-Data Processing with Apache Spark

The processing stage transforms raw records into structured datasets suitable for graph construction and analysis.

The main processing responsibilities are:

1. Read the available raw data from object storage.
2. Filter GitHub Archive events to retain relevant records.
3. Transform and normalize the data into the required schemas.
4. Match package records with relevant GitHub observations.
5. Write the prepared datasets back to RustFS.

The resulting datasets provide the inputs for dependency graph construction and subsequent analytics.

Spark is used for dataset preparation because the raw GitHub Archive input is substantially larger than the filtered dataset. The recorded run contains approximately 15.3 million raw events and 16,800 filtered events.

The project uses Spark 3.5.7 with a Hadoop AWS connector for S3-compatible storage access. The connector version must remain compatible with the Hadoop libraries in the runtime environment.

---

## 7. Dependency Graph Construction

RiskGraph models package relationships as a directed graph.

Let:

\[
G=(V,E)
\]

where:

- \(V\) is the set of package vertices.
- \(E\) is the set of directed dependency edges.

### Graph direction

**A → B means package A depends on package B.**

For example, if `app-package` depends on `utility-package`, the graph contains:

```text
app-package ──────► utility-package
```

The arrow points from the dependent package to the package it uses.

### Graph metrics

**Out-degree**

The out-degree of a package counts its outgoing dependency edges. Under the project's graph convention, it represents the number of dependencies used by that package, subject to how dependency declarations are represented in the graph.

**In-degree**

The in-degree counts incoming edges. It indicates how many other packages in the modeled graph directly depend on a package.

For example:

```text
package-A ───► shared-library
package-B ───► shared-library
package-C ───► shared-library
```

Here, `shared-library` has an in-degree of three.

A package with high in-degree may have broad structural reach because many packages directly depend on it.

**Betweenness centrality**

Betweenness centrality measures how frequently a vertex lies on shortest paths between other vertices. It can help identify packages that occupy structurally important positions in the network.

The project uses sampled betweenness centrality, so the result is an approximation rather than necessarily the exact value for the full graph.

### Why graph analytics matters

Package download counts alone do not reveal all structural relationships. Graph metrics help identify packages that connect to many other packages or occupy potentially important positions in the dependency network.

These metrics describe the modeled network, not the actual runtime behavior of every application.

---

## 8. Heuristic Risk Scoring

RiskGraph combines graph-based information and the project's available observations to assign heuristic risk scores and categories.

The resulting categories are:

- **HIGH:** Packages classified into the highest risk category under the project's scoring rules.
- **MEDIUM:** Packages classified into the intermediate category.
- **LOW:** Packages classified into the lowest category.

The score is intended to support comparative structural analysis within the dataset.

A risk label must not be interpreted as:

- A known security vulnerability.
- A calibrated probability of package failure.
- Proof that a package is unmaintained.
- A prediction that all dependent applications will stop working.

The exact scoring formula, feature weights, thresholds, and treatment of missing observations should be documented alongside the implementation in `analytics/risk_analysis.py`. They should not be inferred solely from the final category counts.

---

## 9. Dependency Cascade Simulation

Cascade simulation explores how a hypothetical package unavailability could affect other packages through the modeled dependency network.

A simulation begins with a selected package and examines the graph according to the implementation's propagation rules.

For example:

```text
application-A ──► library-X
application-B ──► library-X
library-X      ──► utility-Y
```

If `library-X` is selected as the hypothetical unavailable package, the graph can be examined to identify packages that depend on it. With the project's edge direction, incoming edges point toward the selected package from its direct dependents.

This distinction is important: following outgoing edges from `library-X` instead would identify the packages that `library-X` depends on, not the packages that depend on it.

### What the simulation provides

- A way to explore the structural reach of a selected package.
- A means of comparing packages under consistent graph assumptions.
- An illustration of how dependency relationships can create potential propagation paths.

### What it does not provide

The simulation does not establish that an actual outage will occur or that every structurally reachable package will fail. It does not fully model lockfiles, exact version resolution, optional dependencies, runtime behavior, fallback mechanisms, or real outage telemetry.

---

## 10. FastAPI and Streamlit Dashboard

### FastAPI backend

FastAPI serves the analytical outputs through API endpoints, allowing the dashboard and other clients to retrieve project results.

The documented example endpoints are:

- `GET /overview` — retrieve an ecosystem-level summary.
- `GET /package/express` — retrieve information for the `express` package, if available in the generated outputs.

The exact response fields and available endpoints should be verified against the current implementation.

API documentation is available locally at `/docs` and `/redoc`.

### Streamlit dashboard

The Streamlit dashboard provides an interactive interface for exploring the generated results.

Its documented project goals include:

- Viewing ecosystem-level summaries.
- Exploring package risk categories.
- Inspecting dependency graph information.
- Selecting packages for cascade simulations.
- Visualizing analytical outputs using Plotly.

The dashboard is intended to make graph metrics and heuristic risk assessments easier to explore without requiring users to inspect raw datasets or execute analysis scripts manually.

---

## 11. Repository Structure

The expected project structure is:

```text
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

The ingestion scripts may be located in an `ingestion/` directory, depending on the current checkout. Inspect the actual repository before relying on an exact directory or filename.

---

## 12. Prerequisites

Install the following before running the project:

- Docker Desktop with Docker Compose support.
- Git.
- Python 3.11+ for scripts executed directly on the host.
- Java 11 in the Spark environment.
- Sufficient memory for Docker containers and Spark processing.

On a machine with 8 GB RAM, process one Spark stage at a time and use conservative memory settings.

### Clone and enter the repository

```powershell
git clone <repository-url>
cd RiskGraph
```

Replace `<repository-url>` with the actual repository URL.

For an existing Windows checkout, navigate to its repository root, for example:

```powershell
cd C:\Users\saima\.vscode\RiskGraph
```

When using a different computer, replace this example path with the local checkout location.

### Configuration

Review `docker-compose.yml` and any `.env.example` file before starting the services. Create a local `.env` file only when required by the Compose configuration.

Never commit credentials or secrets to version control.

Important configured service addresses:

| Service | Address |
|---|---|
| RustFS inside Docker | `http://rustfs:9000` |
| RustFS from the host | `http://localhost:9000` |
| RustFS Console | `http://localhost:9001` |
| RustFS bucket | `riskgraph` |
| Spark master inside Docker | `spark://spark-master:7077` |
| Spark Master UI | `http://localhost:8081` |

Kafka's internal and host addresses must match the current Compose configuration.

---

## 13. Running the Application

### Step 1: Start the services

From the repository root, run:

```powershell
docker compose up -d --build
```

Check service status and logs:

```powershell
docker compose ps
docker compose logs --tail 100
```

### Step 2: Open the interfaces

Once the services are running, open the following local URLs.

| Interface | URL | Purpose |
|---|---|---|
| RiskGraph Dashboard | `http://localhost:8501` | Explore ecosystem summaries, risk categories, graph information, and cascade results |
| FastAPI Swagger | `http://localhost:8000/docs` | Inspect and test API endpoints |
| FastAPI ReDoc | `http://localhost:8000/redoc` | Alternative API documentation |
| RustFS Console | `http://localhost:9001` | Browse the object-storage bucket and stored data |
| Spark Master UI | `http://localhost:8081` | Monitor Spark workers and applications |

These are local addresses. They work on the computer running the services and are not public internet links.

### Step 3: Check the API

In PowerShell:

```powershell
Invoke-RestMethod http://localhost:8000/overview
Invoke-RestMethod http://localhost:8000/package/express
```

Use the Swagger interface to verify the actual endpoint names, parameters, and response formats in the current version.

---

## 14. Reproducing the Data-Processing Pipeline

The processing stages must run in the correct order. Raw inputs must already be available in RustFS before dataset preparation.

### Stage 1: Prepare datasets

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  processing/prepare_datasets.py
```

This stage reads raw inputs, filters relevant GitHub events, applies the required transformations and matching logic, and writes prepared datasets to object storage.

### Stage 2: Build the dependency graph

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-cores 2 `
  --executor-memory 1536M `
  --driver-memory 768M `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  processing/build_graph.py
```

This stage constructs the package vertices and directed dependency edges from the collected metadata.

### Stage 3: Run analytics

Before running the analytics scripts, inspect their entry points and supported arguments:

```powershell
Get-Content .\analytics\risk_analysis.py -TotalCount 80
Get-Content .\analytics\cascade_simulation.py -TotalCount 80
```

Execute each script using the invocation supported by its implementation.

### Recommended execution order

1. Collect npm and GitHub data.
2. Publish and persist the ingestion records.
3. Prepare and filter datasets.
4. Build the dependency graph.
5. Calculate graph metrics and risk scores.
6. Run cascade simulations.
7. Restart the API and dashboard if they load generated outputs only at startup.

**Important:** Do not rerun data collection merely to reopen the dashboard. Inspect the ingestion producer first, because rerunning collection may append duplicate records or recollect data.

---

## 15. Kafka Topic Setup

The project uses the topics `npm-packages` and `github-events`.

If a topic-creation script is present in the checkout, inspect the ingestion directory:

```powershell
Get-ChildItem .\ingestion -File
```

If `create_topics.py` exists and the required dependencies are installed, run:

```powershell
python .\ingestion\create_topics.py
```

If the directory or script is absent, locate the current Python files using:

```powershell
Get-ChildItem -Recurse -Filter *.py |
    Select-Object -ExpandProperty FullName
```

Confirm that the Kafka service is healthy and that the producer, consumer, and topic configuration use compatible bootstrap addresses.

### Optional host Python environment

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 16. Useful Operational Commands

```powershell
# Inspect services
docker compose ps

# Inspect logs
docker compose logs --tail 100
docker compose logs -f api
docker compose logs -f dashboard
docker compose logs -f kafka
docker compose logs -f rustfs
docker compose logs -f spark-master

# Restart the API and dashboard
docker compose restart api dashboard

# Stop services while retaining containers
docker compose stop

# Start existing containers
docker compose start

# Remove containers and the Compose network
docker compose down
```

`docker compose down` normally preserves named volumes. Avoid `docker compose down -v` unless you intentionally want to remove persisted volumes and their data.

---

## 17. Troubleshooting

### A service is unavailable

Check its status and logs:

```powershell
docker compose ps
docker compose logs --tail 100 <service>
```

Replace `<service>` with the actual Compose service name.

### Spark cannot find S3A classes

Verify that the Hadoop AWS connector is compatible with the Hadoop libraries in the Spark image. The documented setup uses `org.apache.hadoop:hadoop-aws:3.3.4`; version mismatches may cause storage-related errors.

### Docker or Spark uses too much memory

- Run one Spark job at a time.
- Reduce executor resources if necessary.
- Check Docker Desktop and WSL resource limits.
- Avoid running unnecessary services during data processing.

### API results are empty or stale

- Confirm that the expected processed objects exist in RustFS.
- Inspect the `/overview` response.
- Check API logs.
- Verify output filenames and schemas against the API's expectations.
- Restart the API and dashboard if their outputs are loaded only during startup.

### Data collection produces duplicates

Inspect the ingestion scripts and determine whether the producer appends records, overwrites objects, or recollects the entire input. Avoid rerunning ingestion until its behavior is understood.

---

## 18. Limitations and Future Improvements

RiskGraph demonstrates an end-to-end analytical pipeline, but its findings must be interpreted within the limits of its data and assumptions.

### Current limitations

1. **Partial npm coverage:** The project uses a 4,000-package sample rather than the full npm Registry.
2. **Limited observation window:** GitHub activity covers only seven UTC days.
3. **Imperfect matching:** Package-to-repository matching may miss relevant observations.
4. **Approximate graph metrics:** Betweenness centrality is sampled.
5. **Heuristic risk categories:** Scores are project-defined indicators, not calibrated failure probabilities or vulnerability assessments.
6. **Simplified cascade assumptions:** The simulation does not fully model lockfiles, exact version resolution, runtime behavior, or actual outage telemetry.
7. **Missing activity is ambiguous:** A package without matched GitHub observations may still be maintained or used.
8. **Graph completeness:** The observed network is limited by the collected metadata, dependency declarations, and graph-building logic.

### Potential future improvements

- Expand package coverage and repeat collection over longer periods.
- Improve package-to-repository matching and document its confidence.
- Incorporate version-specific dependency relationships.
- Compare multiple graph metrics and validate the risk-scoring methodology.
- Evaluate sensitivity to scoring weights and risk thresholds.
- Compare sampled centrality with exact calculations on smaller graphs.
- Model optional dependencies and alternative dependency-resolution scenarios.
- Validate simulated reach against historical package incidents where suitable data is available.
- Add automated data-quality checks and reproducible pipeline reports.

These are proposed extensions, not claims about features already implemented.

---

## 19. Team Contributions

The project is divided into four presentation workstreams, covering the data pipeline from collection through user-facing visualization.

### Ansita — Data Collection, Ingestion, Kafka and RustFS Storage

**Primary responsibility:** Obtaining source data and making it available to the downstream pipeline.

- Collect npm package metadata and dependency declarations.
- Collect GitHub Archive data for the defined observation window.
- Work on ingestion producers and the separation of records into Kafka topics.
- Support the persistence of raw and collected records in RustFS.
- Help ensure that the data sources are available to the processing pipeline.

**Presentation focus:** Explain where the data comes from, how Kafka separates the incoming record streams, and how RustFS stores the data for subsequent processing.

### Madhuri — Big-Data Processing and Dataset Preparation

**Primary responsibility:** Transforming raw data into structured datasets suitable for analysis.

- Prepare the collected datasets for downstream processing.
- Clean, filter, and transform relevant records.
- Match package information with GitHub observations.
- Write the prepared datasets to object storage.
- Support the transition from raw ingestion records to analytical inputs.

**Presentation focus:** Explain why raw data needs processing, how the GitHub events are filtered, and how the prepared datasets support graph construction.

### Musfira — Dependency Graph, Graph Metrics, Risk Scoring and Cascade Analysis

**Primary responsibility:** Turning processed records into network-level insights.

- Build the directed npm dependency graph.
- Represent packages as vertices and dependency relationships as directed edges.
- Calculate graph metrics, including degree-based measures and sampled betweenness centrality, as implemented.
- Generate heuristic risk scores and package risk categories.
- Perform cascade simulations to explore potential structural reach.

**Presentation focus:** Explain the graph direction, why in-degree matters, how the risk categories should be interpreted, and what cascade simulations can and cannot establish.

### Kavya — FastAPI, Streamlit Dashboard and Integration

**Primary responsibility:** Making the analytical outputs accessible and demonstrating the complete system.

- Develop or integrate the FastAPI backend for serving analytical results.
- Develop or integrate the Streamlit dashboard and visualizations.
- Connect the application to the generated datasets and analytical outputs.
- Support end-to-end integration across the pipeline.
- Prepare the application walkthrough and demonstration.

**Presentation focus:** Demonstrate how users explore package information, risk categories, dependency relationships, and cascade simulation results through the application.

> **Contribution note:** These are the intended presentation workstreams. Each member should verify that the listed tasks reflect their actual work and update the division where responsibilities were shared. Do not present a task as individually completed if it was handled by another member or jointly developed.

---

## 20. Conclusion

RiskGraph demonstrates how big-data processing and graph analytics can be combined to investigate systemic dependency relationships in the JavaScript open-source ecosystem.

By bringing together npm metadata, GitHub activity, Kafka ingestion, RustFS storage, Spark processing, NetworkX analytics, heuristic risk scoring, cascade simulation, and an interactive application, the project provides an end-to-end workflow for exploring package dependency structure.

Its principal contribution is a reproducible analytical prototype that helps investigate which packages occupy potentially significant structural positions in a dependency network. The resulting metrics and simulations are exploratory indicators, and further validation would be required before using them for operational reliability decisions.

---

## License

Add the license selected by the team before publishing the repository.

## Acknowledgements

The project uses the npm Registry, GitHub Archive, Apache Kafka, RustFS, Apache Spark, NetworkX, FastAPI, Streamlit, Plotly, and Docker as part of its data collection, processing, analytics, and presentation workflow.
