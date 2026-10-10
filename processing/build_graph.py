from pyspark.sql import SparkSession, functions as F
import networkx as nx

OUT = "s3a://riskgraph/processed"

spark = (
    SparkSession.builder
    .appName("RiskGraph-Build-Graph")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    .config("spark.hadoop.fs.s3a.access.key", "riskgraph")
    .config("spark.hadoop.fs.s3a.secret.key", "riskgraph_local_password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("========================================")
print("READING PROCESSED DATA")
print("========================================")

package_enriched = spark.read.parquet(
    f"{OUT}/package_enriched"
)

edges = (
    spark.read.parquet(
        f"{OUT}/dependency_edges"
    )
    .select(
        "src",
        "dst",
        "required_version",
        "dependency_type",
    )
    .filter(
        F.col("src").isNotNull()
        & F.col("dst").isNotNull()
        & (F.col("src") != F.col("dst"))
    )
    .dropDuplicates(
        ["src", "dst"]
    )
)

package_count = package_enriched.count()
edge_count = edges.count()

print(
    "Package enriched:",
    package_count,
)

print(
    "Dependency edges:",
    edge_count,
)


print("========================================")
print("BUILDING OBSERVED PACKAGE VERTICES")
print("========================================")

observed_vertices = (
    package_enriched
    .select(
        F.col("package_name").alias("id"),
        "version",
        "description",
        "repository_key",
        F.col(
            "github_event_count"
        ).alias("event_count"),
        F.col(
            "github_active_contributors"
        ).alias("active_contributors"),
        F.col(
            "github_commit_activity"
        ).alias("commit_count"),
        "bus_factor",
        F.col(
            "avg_issue_response_hours"
        ).alias(
            "avg_issue_first_response_hours"
        ),
        F.col(
            "last_event_at"
        ).alias(
            "last_observed_event"
        ),
    )
    .withColumn(
        "is_observed_package",
        F.lit(True),
    )
    .dropDuplicates(
        ["id"]
    )
)

observed_count = observed_vertices.count()

print(
    "Observed npm packages:",
    observed_count,
)


print("========================================")
print("BUILDING COMPLETE GRAPH VERTEX SET")
print("========================================")

all_ids = (
    observed_vertices
    .select("id")
    .union(
        edges.select(
            F.col("src").alias("id")
        )
    )
    .union(
        edges.select(
            F.col("dst").alias("id")
        )
    )
    .distinct()
)

vertices = (
    all_ids
    .join(
        observed_vertices,
        "id",
        "left",
    )
    .withColumn(
        "is_observed_package",
        F.coalesce(
            F.col(
                "is_observed_package"
            ),
            F.lit(False),
        ),
    )
)

vertex_count = vertices.count()

print(
    "Graph vertices:",
    vertex_count,
)

print(
    "Graph edges:",
    edge_count,
)

print(
    "Observed packages represented:",
    observed_count,
)

print("========================================")
print("WRITING GRAPH STRUCTURE")
print("========================================")

vertices.write.mode(
    "overwrite"
).parquet(
    f"{OUT}/graph_vertices"
)

edges.write.mode(
    "overwrite"
).parquet(
    f"{OUT}/graph_edges"
)

print("Graph structure written.")


print("========================================")
print("BUILDING NETWORKX GRAPH")
print("========================================")

edge_rows = (
    edges
    .select(
        "src",
        "dst",
    )
    .collect()
)

nx_graph = nx.DiGraph()

nx_graph.add_edges_from(
    [
        (
            row["src"],
            row["dst"],
        )
        for row in edge_rows
    ]
)

# Add isolated observed packages too.
observed_ids = (
    observed_vertices
    .select("id")
    .collect()
)

nx_graph.add_nodes_from(
    [
        row["id"]
        for row in observed_ids
    ]
)

print(
    "NetworkX nodes:",
    nx_graph.number_of_nodes(),
)

print(
    "NetworkX edges:",
    nx_graph.number_of_edges(),
)


print("========================================")
print("CALCULATING DEGREE METRICS")
print("========================================")

in_degree_dict = dict(
    nx_graph.in_degree()
)

out_degree_dict = dict(
    nx_graph.out_degree()
)

print(
    "Degree metrics complete."
)


print("========================================")
print("CALCULATING PAGERANK")
print("========================================")

pagerank_dict = nx.pagerank(
    nx_graph,
    alpha=0.85,
    max_iter=200,
    tol=1.0e-8,
)

print(
    "PageRank complete."
)


print("========================================")
print("CALCULATING SAMPLED BETWEENNESS")
print("========================================")

num_nodes = (
    nx_graph.number_of_nodes()
)

if num_nodes > 0:

    sample_k = min(
        250,
        num_nodes,
    )

    print(
        "Sampling",
        sample_k,
        "source nodes out of",
        num_nodes,
    )

    betweenness_dict = (
        nx.betweenness_centrality(
            nx_graph,
            k=sample_k,
            normalized=True,
            seed=42,
        )
    )

else:

    betweenness_dict = {}


print(
    "Betweenness complete."
)


print("========================================")
print("CALCULATING COMMUNITIES")
print("========================================")

community_graph = (
    nx_graph.to_undirected()
)

communities = list(
    nx.community.label_propagation_communities(
        community_graph
    )
)

community_dict = {}

for community_id, community in enumerate(
    communities
):

    for node in community:

        community_dict[node] = (
            community_id
        )

print(
    "Communities found:",
    len(communities),
)


print("========================================")
print("CALCULATING K-CORE")
print("========================================")

try:

    core_dict = nx.core_number(
        community_graph
    )

    print(
        "K-core calculation complete."
    )

except Exception as exc:

    print(
        "K-core calculation unavailable:",
        exc,
    )

    core_dict = {}


print("========================================")
print("CREATING GRAPH METRICS")
print("========================================")

metric_rows = []

for node in nx_graph.nodes():

    metric_rows.append(
        (
            node,

            int(
                in_degree_dict.get(
                    node,
                    0,
                )
            ),

            int(
                out_degree_dict.get(
                    node,
                    0,
                )
            ),

            float(
                pagerank_dict.get(
                    node,
                    0.0,
                )
            ),

            float(
                betweenness_dict.get(
                    node,
                    0.0,
                )
            ),

            int(
                community_dict.get(
                    node,
                    -1,
                )
            ),

            int(
                core_dict.get(
                    node,
                    0,
                )
            ),
        )
    )


metrics_df = spark.createDataFrame(
    metric_rows,
    [
        "id",
        "in_degree",
        "out_degree",
        "pagerank",
        "betweenness_sampled",
        "community_id",
        "kcore",
    ],
)


print("========================================")
print("COMBINING GRAPH + PACKAGE ATTRIBUTES")
print("========================================")

metrics = (
    vertices
    .join(
        metrics_df,
        "id",
        "left",
    )
    .fillna(
        {
            "in_degree": 0,
            "out_degree": 0,
            "pagerank": 0.0,
            "betweenness_sampled": 0.0,
            "community_id": -1,
            "kcore": 0,
        }
    )
)


print("Writing graph metrics...")


metrics.write.mode(
    "overwrite"
).parquet(
    f"{OUT}/graph_metrics"
)


print("========================================")
print("GRAPH ANALYTICS COMPLETE")
print("========================================")

print(
    "Graph vertices:",
    nx_graph.number_of_nodes(),
)

print(
    "Graph edges:",
    nx_graph.number_of_edges(),
)

print(
    "Observed npm packages:",
    observed_count,
)

print(
    "Communities:",
    len(communities),
)


print("========================================")
print("TOP PACKAGES BY PAGERANK")
print("========================================")

(
    metrics
    .filter(
        F.col(
            "is_observed_package"
        ) == True
    )
    .orderBy(
        F.desc("pagerank")
    )
    .select(
        "id",
        "in_degree",
        "out_degree",
        "pagerank",
        "betweenness_sampled",
        "community_id",
        "kcore",
        "event_count",
        "active_contributors",
        "commit_count",
        "bus_factor",
        "avg_issue_first_response_hours",
        "is_observed_package",
    )
    .show(
        30,
        truncate=False,
    )
)


print("========================================")
print("GRAPH OUTPUTS")
print("========================================")

print(
    f"{OUT}/graph_vertices"
)

print(
    f"{OUT}/graph_edges"
)

print(
    f"{OUT}/graph_metrics"
)

print("========================================")
print("DONE")
print("========================================")

spark.stop()