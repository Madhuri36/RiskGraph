from pyspark.sql import SparkSession, functions as F

OUT = "s3a://riskgraph/processed"

spark = (
    SparkSession.builder
    .appName("RiskGraph-Finish-Datasets")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    .config("spark.hadoop.fs.s3a.access.key", "riskgraph")
    .config("spark.hadoop.fs.s3a.secret.key", "riskgraph_local_password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("========================================")
print("READING EXISTING PROCESSED DATA")
print("========================================")

package_nodes = spark.read.parquet(f"{OUT}/package_nodes")
filtered = spark.read.parquet(f"{OUT}/github_filtered_events")

print("Package nodes:", package_nodes.count())
print("Filtered GitHub events:", filtered.count())

# ---------------------------------------------------------
# REPOSITORY ACTIVITY
# ---------------------------------------------------------

print("========================================")
print("CALCULATING REPOSITORY ACTIVITY")
print("========================================")

repo_activity = (
    filtered
    .groupBy("repository_key")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct(
            F.when(
                F.col("event_type") == "PushEvent",
                F.col("event_id")
            )
        ).alias("push_event_count"),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("event_type") == "PushEvent",
                    F.coalesce(F.col("push_size"), F.lit(0))
                )
            ),
            F.lit(0)
        ).alias("commit_activity"),
        F.countDistinct(
            F.when(
                F.col("event_type") == "PushEvent",
                F.col("actor_login")
            )
        ).alias("active_contributors"),
        F.min("created_at").alias("first_event_at"),
        F.max("created_at").alias("last_event_at")
    )
)

repo_activity.write.mode("overwrite").parquet(
    f"{OUT}/github_repo_activity"
)

# ---------------------------------------------------------
# CONTRIBUTORS
# ---------------------------------------------------------

print("========================================")
print("CALCULATING CONTRIBUTORS")
print("========================================")

contributors = (
    filtered
    .filter(F.col("actor_login").isNotNull())
    .groupBy("repository_key", "actor_login")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct(
            F.when(
                F.col("event_type") == "PushEvent",
                F.col("event_id")
            )
        ).alias("push_events")
    )
)

contributors.write.mode("overwrite").parquet(
    f"{OUT}/github_contributors"
)

# ---------------------------------------------------------
# BUS FACTOR
# ---------------------------------------------------------

print("========================================")
print("CALCULATING BUS FACTOR")
print("========================================")

pushes = (
    filtered
    .filter(
        (F.col("event_type") == "PushEvent")
        & F.col("actor_login").isNotNull()
    )
    .groupBy("repository_key", "actor_login")
    .agg(F.count("*").alias("push_count"))
)

print("Collecting contributor push counts...")

push_rows = pushes.collect()

repo_contributors = {}

for row in push_rows:
    repo = row["repository_key"]
    count = row["push_count"]

    if repo not in repo_contributors:
        repo_contributors[repo] = []

    repo_contributors[repo].append(count)

bus_factor_rows = []

for repo, counts in repo_contributors.items():
    counts.sort(reverse=True)

    total = sum(counts)

    if total == 0:
        bus_factor = 0
    else:
        cumulative = 0
        bus_factor = 0

        for count in counts:
            cumulative += count
            bus_factor += 1

            if cumulative / total >= 0.5:
                break

    bus_factor_rows.append((repo, bus_factor))

bus_factor_schema = "repository_key string, bus_factor long"

bus_factor = spark.createDataFrame(
    bus_factor_rows,
    schema=bus_factor_schema
)

bus_factor.write.mode("overwrite").parquet(
    f"{OUT}/github_bus_factor"
)

print("Bus factor repositories:", len(bus_factor_rows))

# ---------------------------------------------------------
# ISSUE RESPONSE
# ---------------------------------------------------------

print("========================================")
print("CALCULATING APPROXIMATE ISSUE RESPONSE")
print("========================================")

issues = (
    filtered
    .filter(
        (F.col("event_type") == "IssuesEvent")
        & (F.col("action") == "opened")
        & F.col("issue_number").isNotNull()
        & F.col("issue_created_at").isNotNull()
    )
    .select(
        "repository_key",
        "issue_number",
        F.to_timestamp("issue_created_at").alias("issue_created_ts")
    )
    .dropDuplicates(
        ["repository_key", "issue_number"]
    )
)

comments = (
    filtered
    .filter(
        (F.col("event_type") == "IssueCommentEvent")
        & F.col("issue_number").isNotNull()
    )
    .select(
        "repository_key",
        "issue_number",
        F.to_timestamp("created_at").alias("comment_ts")
    )
)

first_comment = (
    comments
    .join(
        issues,
        ["repository_key", "issue_number"],
        "inner"
    )
    .filter(
        F.col("comment_ts") >= F.col("issue_created_ts")
    )
    .groupBy(
        "repository_key",
        "issue_number",
        "issue_created_ts"
    )
    .agg(
        F.min("comment_ts").alias("first_comment_ts")
    )
    .withColumn(
        "response_hours",
        (
            F.col("first_comment_ts").cast("long")
            - F.col("issue_created_ts").cast("long")
        ) / 3600.0
    )
)

issue_response = (
    first_comment
    .groupBy("repository_key")
    .agg(
        F.avg("response_hours").alias(
            "avg_issue_response_hours"
        ),
        F.count("*").alias(
            "issues_with_observed_response"
        )
    )
)

issue_response.write.mode("overwrite").parquet(
    f"{OUT}/github_issue_response"
)

# ---------------------------------------------------------
# PACKAGE ENRICHMENT
# ---------------------------------------------------------

print("========================================")
print("BUILDING PACKAGE ENRICHED")
print("========================================")

package_enriched = (
    package_nodes.alias("p")
    .join(
        repo_activity.alias("a"),
        "repository_key",
        "left"
    )
    .join(
        bus_factor.alias("b"),
        "repository_key",
        "left"
    )
    .join(
        issue_response.alias("i"),
        "repository_key",
        "left"
    )
    .select(
        F.col("p.package_name").alias("package_name"),
        F.col("p.version").alias("version"),
        F.col("p.description").alias("description"),
        F.col("p.repository_key").alias("repository_key"),
        F.col("p.collected_at").alias("collected_at"),
        F.col("p.dependency_count").alias("dependency_count"),
        F.col("p.dev_dependency_count").alias(
            "dev_dependency_count"
        ),
        F.col("p.npm_maintainer_count").alias(
            "npm_maintainer_count"
        ),
        F.coalesce(
            F.col("a.event_count"),
            F.lit(0)
        ).alias("github_event_count"),
        F.coalesce(
            F.col("a.push_event_count"),
            F.lit(0)
        ).alias("github_push_event_count"),
        F.coalesce(
            F.col("a.commit_activity"),
            F.lit(0)
        ).alias("github_commit_activity"),
        F.coalesce(
            F.col("a.active_contributors"),
            F.lit(0)
        ).alias("github_active_contributors"),
        F.col("a.first_event_at").alias(
            "first_event_at"
        ),
        F.col("a.last_event_at").alias(
            "last_event_at"
        ),
        F.coalesce(
            F.col("b.bus_factor"),
            F.lit(0)
        ).alias("bus_factor"),
        F.col("i.avg_issue_response_hours").alias(
            "avg_issue_response_hours"
        ),
        F.coalesce(
            F.col("i.issues_with_observed_response"),
            F.lit(0)
        ).alias(
            "issues_with_observed_response"
        )
    )
)

package_enriched.write.mode("overwrite").parquet(
    f"{OUT}/package_enriched"
)

print("========================================")
print("RISKGRAPH DATA PREPARATION COMPLETE")
print("========================================")

print("Packages:", package_nodes.count())
print("Filtered GitHub events:", filtered.count())
print("Repositories:", repo_activity.count())
print("Enriched packages:", package_enriched.count())

print("Output:", OUT)

print("========================================")

spark.stop()