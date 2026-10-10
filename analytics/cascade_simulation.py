from pyspark.sql import SparkSession, functions as F
from collections import defaultdict, deque

OUT = "s3a://riskgraph/processed"

TOP_N = 20
MAX_HOPS = 25

spark = (
    SparkSession.builder
    .appName("RiskGraph-Cascade-Simulation")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    .config("spark.hadoop.fs.s3a.access.key", "riskgraph")
    .config("spark.hadoop.fs.s3a.secret.key", "riskgraph_local_password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.maximum", "50")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 70)
print("RISKGRAPH CASCADE SIMULATION")
print("=" * 70)

# ---------------------------------------------------------
# LOAD RISK SCORES
# ---------------------------------------------------------

risk_df = (
    spark.read.parquet(f"{OUT}/risk_scores")
    .select(
        "id",
        "final_risk_score",
        "risk_level",
    )
    .dropDuplicates(["id"])
)

risk_rows = risk_df.collect()

risk_lookup = {}

for row in risk_rows:
    risk_lookup[row["id"]] = {
        "score": (
            float(row["final_risk_score"])
            if row["final_risk_score"] is not None
            else None
        ),
        "level": row["risk_level"],
    }

print(f"Risk packages loaded: {len(risk_lookup)}")


# ---------------------------------------------------------
# LOAD DEPENDENCY EDGES
# ---------------------------------------------------------

edges_df = (
    spark.read.parquet(f"{OUT}/graph_edges")
    .select("src", "dst")
    .filter(
        F.col("src").isNotNull()
        & F.col("dst").isNotNull()
        & (F.col("src") != F.col("dst"))
    )
    .dropDuplicates()
)

edge_rows = edges_df.collect()

print(f"Dependency edges loaded: {len(edge_rows)}")


# ---------------------------------------------------------
# BUILD REVERSE DEPENDENCY GRAPH
# ---------------------------------------------------------

# Original:
#
# A -> B
#
# means:
# A depends on B
#
# If B fails:
#
# B -> A
#
# therefore we build:
#
# dependency -> dependents

reverse_graph = defaultdict(set)

for row in edge_rows:
    package = row["src"]
    dependency = row["dst"]

    reverse_graph[dependency].add(package)

print(
    f"Reverse dependency nodes: {len(reverse_graph)}"
)


# ---------------------------------------------------------
# SELECT TOP RISK PACKAGES
# ---------------------------------------------------------

top_packages = (
    risk_df
    .filter(F.col("final_risk_score").isNotNull())
    .orderBy(F.desc("final_risk_score"))
    .limit(TOP_N)
    .collect()
)

print(f"Failure seeds selected: {len(top_packages)}")


# ---------------------------------------------------------
# CASCADE BFS
# ---------------------------------------------------------

summary_rows = []
affected_rows = []

for index, root in enumerate(top_packages, start=1):

    root_id = root["id"]
    root_risk = float(root["final_risk_score"])
    root_level = root["risk_level"]

    print(
        f"\n[{index}/{len(top_packages)}] "
        f"{root_id} "
        f"(risk={root_risk:.4f}, level={root_level})"
    )

    # package -> minimum cascade depth
    visited = {
        root_id: 0
    }

    queue = deque([
        (root_id, 0)
    ])

    while queue:

        current, depth = queue.popleft()

        if depth >= MAX_HOPS:
            continue

        for dependent in reverse_graph.get(current, set()):

            if dependent in visited:
                continue

            next_depth = depth + 1

            visited[dependent] = next_depth

            queue.append(
                (dependent, next_depth)
            )

    # Remove failed package itself.
    affected = [
        (node, depth)
        for node, depth in visited.items()
        if node != root_id
    ]

    directly_affected = sum(
        1
        for _, depth in affected
        if depth == 1
    )

    total_affected = len(affected)

    cascade_depth = max(
        (depth for _, depth in affected),
        default=0
    )

    observed_affected = 0
    high_risk_affected = 0
    risk_weighted_impact = 0.0

    for node, depth in affected:

        info = risk_lookup.get(node)

        if info is not None:

            score = info["score"]
            level = info["level"]

            if score is not None:
                observed_affected += 1
                risk_weighted_impact += score

            if level == "HIGH":
                high_risk_affected += 1

        else:
            score = None
            level = None

        affected_rows.append(
            (
                root_id,
                node,
                int(depth),
                score,
                level,
                1 if level == "HIGH" else 0,
            )
        )

    summary_rows.append(
        (
            root_id,
            root_risk,
            root_level,
            int(directly_affected),
            int(total_affected),
            int(observed_affected),
            int(high_risk_affected),
            int(cascade_depth),
            float(risk_weighted_impact),
        )
    )

    print(
        f"  Directly affected : {directly_affected}"
    )

    print(
        f"  Total affected    : {total_affected}"
    )

    print(
        f"  Observed packages : {observed_affected}"
    )

    print(
        f"  High-risk affected: {high_risk_affected}"
    )

    print(
        f"  Cascade depth     : {cascade_depth}"
    )

    print(
        f"  Risk-weighted     : {risk_weighted_impact:.4f}"
    )


# ---------------------------------------------------------
# CREATE SUMMARY DATAFRAME
# ---------------------------------------------------------

summary = spark.createDataFrame(
    summary_rows,
    [
        "failed_package",
        "risk_score",
        "risk_level",
        "directly_affected",
        "total_affected",
        "observed_affected",
        "high_risk_affected",
        "cascade_depth",
        "risk_weighted_impact",
    ],
)

summary = summary.orderBy(
    F.desc("total_affected")
)


# ---------------------------------------------------------
# WRITE CASCADE SUMMARY
# ---------------------------------------------------------

summary.write.mode("overwrite").parquet(
    f"{OUT}/cascade_summary"
)


# ---------------------------------------------------------
# CREATE AFFECTED NODE DATAFRAME
# ---------------------------------------------------------

if affected_rows:

    affected_df = spark.createDataFrame(
        affected_rows,
        [
            "failed_package",
            "id",
            "depth",
            "final_risk_score",
            "risk_level",
            "is_high_risk",
        ],
    )

    (
        affected_df
        .dropDuplicates(
            ["failed_package", "id"]
        )
        .write
        .mode("overwrite")
        .parquet(
            f"{OUT}/cascade_affected_nodes"
        )
    )


# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("CASCADE RESULTS")
print("=" * 70)

summary.show(
    TOP_N,
    truncate=False,
)

print("\nOutputs:")
print(
    f"{OUT}/cascade_summary"
)

print(
    f"{OUT}/cascade_affected_nodes"
)

print("\n" + "=" * 70)
print("CASCADE SIMULATION COMPLETE")
print("=" * 70)

spark.stop()