import io
import os
from functools import lru_cache

import networkx as nx
import pandas as pd
from fastapi import FastAPI, HTTPException
from minio import Minio


app = FastAPI(
    title="RiskGraph API",
    description="Systemic npm dependency risk analytics API",
    version="1.0.0",
)


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "rustfs:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "riskgraph")
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "riskgraph_local_password",
)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "riskgraph")


client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


DATASETS = {
    "package_enriched": "processed/package_enriched/",
    "graph_vertices": "processed/graph_vertices/",
    "graph_edges": "processed/graph_edges/",
    "graph_metrics": "processed/graph_metrics/",
    "risk_scores": "processed/risk_scores/",
    "cascade_summary": "processed/cascade_summary/",
    "cascade_affected_nodes": "processed/cascade_affected_nodes/",
}


def read_parquet_prefix(prefix):
    frames = []

    objects = client.list_objects(
        MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue

        response = None

        try:
            response = client.get_object(
                MINIO_BUCKET,
                obj.object_name,
            )

            data = response.read()

            if data:
                frames.append(
                    pd.read_parquet(
                        io.BytesIO(data)
                    )
                )

        finally:
            if response:
                response.close()
                response.release_conn()

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    )


def normalize_risk(df):
    if df.empty:
        return df

    df = df.copy()

    if "package_name" not in df.columns:
        if "id" in df.columns:
            df["package_name"] = df["id"]

    return df


def normalize_cascade(df):
    if df.empty:
        return df

    df = df.copy()

    if "package_name" not in df.columns:
        if "failed_package" in df.columns:
            df["package_name"] = df["failed_package"]

    return df


@lru_cache(maxsize=1)
def risk_scores():
    return normalize_risk(
        read_parquet_prefix(
            DATASETS["risk_scores"]
        )
    )


@lru_cache(maxsize=1)
def graph_vertices():
    return read_parquet_prefix(
        DATASETS["graph_vertices"]
    )


@lru_cache(maxsize=1)
def graph_edges():
    return read_parquet_prefix(
        DATASETS["graph_edges"]
    )


@lru_cache(maxsize=1)
def package_enriched():
    return read_parquet_prefix(
        DATASETS["package_enriched"]
    )


@lru_cache(maxsize=1)
def graph_metrics():
    return read_parquet_prefix(
        DATASETS["graph_metrics"]
    )


@lru_cache(maxsize=1)
def cascade_summary():
    return normalize_cascade(
        read_parquet_prefix(
            DATASETS["cascade_summary"]
        )
    )


@lru_cache(maxsize=1)
def cascade_affected():
    return read_parquet_prefix(
        DATASETS["cascade_affected_nodes"]
    )


def clean_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "isoformat"):
        return value.isoformat()

    try:
        return value.item()
    except Exception:
        return value


def clean_row(row):
    return {
        key: clean_value(value)
        for key, value in row.items()
    }


@app.get("/")
def root():
    return {
        "name": "RiskGraph API",
        "status": "running",
        "version": "1.0.0",
    }


@app.get("/health")
def health():
    try:
        bucket_exists = client.bucket_exists(
            MINIO_BUCKET
        )
    except Exception:
        bucket_exists = False

    return {
        "status": (
            "healthy"
            if bucket_exists
            else "degraded"
        ),
        "rustfs": bucket_exists,
        "bucket": MINIO_BUCKET,
    }


@app.get("/overview")
def overview():
    risk = risk_scores()
    vertices = graph_vertices()
    edges = graph_edges()

    if risk.empty:
        raise HTTPException(
            status_code=404,
            detail="Risk score dataset is empty",
        )

    packages = (
        risk["package_name"].nunique()
        if "package_name" in risk.columns
        else 0
    )

    high = 0
    medium = 0
    low = 0

    if "risk_level" in risk.columns:
        levels = (
            risk["risk_level"]
            .astype(str)
            .str.upper()
        )

        high = int(
            (levels == "HIGH").sum()
        )

        medium = int(
            (levels == "MEDIUM").sum()
        )

        low = int(
            (levels == "LOW").sum()
        )

    github_observed = 0

    if "has_github_observation" in risk.columns:
        github_observed = int(
            risk["has_github_observation"]
            .fillna(False)
            .astype(bool)
            .sum()
        )

    return {
        "packages": int(packages),
        "graph_nodes": int(len(vertices)),
        "graph_edges": int(len(edges)),
        "high_risk": high,
        "medium_risk": medium,
        "low_risk": low,
        "github_observed": github_observed,
    }


@app.get("/risk/distribution")
def risk_distribution():
    risk = risk_scores()

    if risk.empty:
        return []

    counts = (
        risk["risk_level"]
        .astype(str)
        .str.upper()
        .value_counts()
    )

    return [
        {
            "risk_level": str(level),
            "count": int(count),
        }
        for level, count in counts.items()
    ]


@app.get("/risk/top")
def top_risk(limit: int = 20):
    risk = risk_scores()

    if risk.empty:
        return []

    if "final_risk_score" in risk.columns:
        risk = risk.sort_values(
            "final_risk_score",
            ascending=False,
        )

    limit = max(1, min(limit, 100))

    return [
        clean_row(row)
        for _, row in risk.head(limit).iterrows()
    ]


@app.get("/package/{package_name}")
def package_details(package_name: str):
    risk = risk_scores()

    rows = risk[
        risk["package_name"].astype(str)
        == package_name
    ]

    if rows.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Package '{package_name}' not found",
        )

    result = clean_row(
        rows.iloc[0]
    )

    enriched = package_enriched()

    if (
        not enriched.empty
        and "package_name" in enriched.columns
    ):
        extra_rows = enriched[
            enriched["package_name"].astype(str)
            == package_name
        ]

        if not extra_rows.empty:
            extra = clean_row(
                extra_rows.iloc[0]
            )

            for key, value in extra.items():
                if key not in result:
                    result[key] = value

    return result


def build_dependency_graph():
    edges = graph_edges()

    graph = nx.DiGraph()

    if edges.empty:
        return graph

    for _, row in edges.iterrows():
        src = str(row["src"])
        dst = str(row["dst"])

        graph.add_edge(src, dst)

    return graph


@app.get("/package/{package_name}/dependencies")
def dependencies(
    package_name: str,
    depth: int = 1,
):
    graph = build_dependency_graph()

    depth = max(
        1,
        min(depth, 3),
    )

    visited = {
        package_name: 0
    }

    if package_name in graph:
        queue = [package_name]

        while queue:
            current = queue.pop(0)
            current_depth = visited[current]

            if current_depth >= depth:
                continue

            for dependency in graph.successors(
                current
            ):
                if dependency not in visited:
                    visited[dependency] = (
                        current_depth + 1
                    )
                    queue.append(dependency)

    nodes = [
        {
            "id": node,
            "depth": node_depth,
        }
        for node, node_depth in visited.items()
    ]

    selected = set(visited.keys())

    result_edges = [
        {
            "source": src,
            "target": dst,
        }
        for src, dst in graph.edges()
        if src in selected
        and dst in selected
    ]

    return {
        "package": package_name,
        "nodes": nodes,
        "edges": result_edges,
    }


@app.get("/package/{package_name}/dependents")
def dependents(
    package_name: str,
    depth: int = 1,
):
    graph = build_dependency_graph()
    reverse = graph.reverse()

    depth = max(
        1,
        min(depth, 3),
    )

    visited = {
        package_name: 0
    }

    if package_name in reverse:
        queue = [package_name]

        while queue:
            current = queue.pop(0)
            current_depth = visited[current]

            if current_depth >= depth:
                continue

            for dependent in reverse.successors(
                current
            ):
                if dependent not in visited:
                    visited[dependent] = (
                        current_depth + 1
                    )
                    queue.append(dependent)

    selected = set(visited.keys())

    nodes = [
        {
            "id": node,
            "depth": node_depth,
        }
        for node, node_depth in visited.items()
    ]

    result_edges = [
        {
            "source": src,
            "target": dst,
        }
        for src, dst in graph.edges()
        if src in selected
        and dst in selected
    ]

    return {
        "package": package_name,
        "nodes": nodes,
        "edges": result_edges,
    }


@app.get("/cascade/summary")
def cascade():
    df = cascade_summary()

    if df.empty:
        return []

    if "total_affected" in df.columns:
        df = df.sort_values(
            "total_affected",
            ascending=False,
        )

    return [
        clean_row(row)
        for _, row in df.iterrows()
    ]


@app.get("/cascade/{package_name}")
def cascade_package(package_name: str):
    summary = cascade_summary()

    rows = summary[
        summary["package_name"].astype(str)
        == package_name
    ]

    if rows.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Cascade result for "
                f"'{package_name}' not found"
            ),
        )

    affected = cascade_affected()

    affected_rows = pd.DataFrame()

    if not affected.empty:
        if "failed_package" in affected.columns:
            affected_rows = affected[
                affected["failed_package"].astype(str)
                == package_name
            ]
        elif "seed_package" in affected.columns:
            affected_rows = affected[
                affected["seed_package"].astype(str)
                == package_name
            ]
        elif "package_name" in affected.columns:
            affected_rows = affected[
                affected["package_name"].astype(str)
                == package_name
            ]

    return {
        "summary": clean_row(
            rows.iloc[0]
        ),
        "affected_nodes": [
            clean_row(row)
            for _, row in affected_rows.iterrows()
        ],
    }


@app.post("/cache/clear")
def clear_cache():
    risk_scores.cache_clear()
    graph_vertices.cache_clear()
    graph_edges.cache_clear()
    package_enriched.cache_clear()
    graph_metrics.cache_clear()
    cascade_summary.cache_clear()
    cascade_affected.cache_clear()

    return {
        "status": "cache cleared"
    }