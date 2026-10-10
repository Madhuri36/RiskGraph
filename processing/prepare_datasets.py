from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window


RAW_NPM = "s3a://riskgraph/raw/npm-packages/"
RAW_GITHUB = "s3a://riskgraph/raw/github-events/"
OUT = "s3a://riskgraph/processed"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RiskGraph-Prepare-Datasets")

    # RustFS / S3A
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://rustfs:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "riskgraph"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "riskgraph_local_password"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # Spark performance
    .config(
        "spark.sql.adaptive.enabled",
        "true"
    )
    .config(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true"
    )
    .config(
        "spark.sql.adaptive.skewJoin.enabled",
        "true"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )
    .config(
        "spark.sql.files.maxPartitionBytes",
        "134217728"
    )
    .config(
        "spark.sql.files.openCostInBytes",
        "16777216"
    )
    .config(
        "spark.sql.hive.filesourcePartitionFileCacheSize",
        "67108864"
    )
    .config(
        "spark.sql.parquet.compression.codec",
        "snappy"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPER
# ============================================================

def normalize_repo(col):
    x = F.lower(F.trim(col))

    x = F.regexp_replace(
        x,
        r"^git\+",
        ""
    )

    x = F.regexp_replace(
        x,
        r"^https?://",
        ""
    )

    x = F.regexp_replace(
        x,
        r"^ssh://git@",
        ""
    )

    x = F.regexp_replace(
        x,
        r"^git@",
        ""
    )

    x = F.regexp_replace(
        x,
        r"^www\.",
        ""
    )

    x = F.regexp_replace(
        x,
        r"^github\.com[:/]",
        "github.com/"
    )

    x = F.regexp_replace(
        x,
        r"\.git$",
        ""
    )

    x = F.regexp_replace(
        x,
        r"/+$",
        ""
    )

    return x


map_type = T.MapType(
    T.StringType(),
    T.StringType()
)

maintainer_type = T.ArrayType(
    T.StructType([
        T.StructField(
            "name",
            T.StringType(),
            True
        ),
        T.StructField(
            "email",
            T.StringType(),
            True
        )
    ])
)


# ============================================================
# 1. READ NPM DATA
# ============================================================

print("")
print("========================================")
print("READING NPM RAW JSONL")
print("========================================")

npm_raw = spark.read.text(RAW_NPM)

repo_json = F.get_json_object(
    "value",
    "$.repository"
)

repo_url = F.coalesce(
    F.get_json_object(
        "value",
        "$.repository.url"
    ),
    F.get_json_object(
        repo_json,
        "$.url"
    ),
    F.regexp_replace(
        repo_json,
        r'^"|"$',
        ""
    )
)

npm = (
    npm_raw
    .select(
        F.get_json_object(
            "value",
            "$.name"
        ).alias("package_name"),

        F.get_json_object(
            "value",
            "$.version"
        ).alias("version"),

        F.get_json_object(
            "value",
            "$.description"
        ).alias("description"),

        F.get_json_object(
            "value",
            "$.dependencies"
        ).alias("dependencies_json"),

        F.get_json_object(
            "value",
            "$.devDependencies"
        ).alias("dev_dependencies_json"),

        F.get_json_object(
            "value",
            "$.maintainers"
        ).alias("maintainers_json"),

        repo_url.alias("repository_url"),

        F.get_json_object(
            "value",
            "$.collected_at"
        ).alias("collected_at")
    )
    .filter(
        F.col("package_name").isNotNull()
    )
    .withColumn(
        "repository_key",
        normalize_repo(
            F.col("repository_url")
        )
    )
)


# ============================================================
# 2. CREATE PACKAGE NODES
# ============================================================

print("")
print("Creating package nodes...")

package_nodes = (
    npm

    .withColumn(
        "dependencies_map",
        F.from_json(
            F.col("dependencies_json"),
            map_type
        )
    )

    .withColumn(
        "dev_dependencies_map",
        F.from_json(
            F.col("dev_dependencies_json"),
            map_type
        )
    )

    .withColumn(
        "maintainers_array",
        F.from_json(
            F.col("maintainers_json"),
            maintainer_type
        )
    )

    .select(
        "package_name",
        "version",
        "description",
        "repository_key",
        "collected_at",

        F.coalesce(
            F.size("dependencies_map"),
            F.lit(0)
        ).alias(
            "dependency_count"
        ),

        F.coalesce(
            F.size("dev_dependencies_map"),
            F.lit(0)
        ).alias(
            "dev_dependency_count"
        ),

        F.coalesce(
            F.size("maintainers_array"),
            F.lit(0)
        ).alias(
            "npm_maintainer_count"
        )
    )

    .dropDuplicates(
        ["package_name"]
    )
)

npm_count = package_nodes.count()

print(
    "NPM package count:",
    npm_count
)


# ============================================================
# 3. CREATE DEPENDENCY EDGES
# ============================================================

print("")
print("Creating dependency edges...")

dependency_edges = (
    npm

    .select(
        F.col("package_name").alias("src"),

        F.from_json(
            F.col("dependencies_json"),
            map_type
        ).alias("dependencies")
    )

    .select(
        "src",

        F.explode_outer(
            "dependencies"
        ).alias(
            "dst",
            "required_version"
        )
    )

    .filter(
        F.col("dst").isNotNull()
    )

    .withColumn(
        "dependency_type",
        F.lit("runtime")
    )

    .select(
        "src",
        "dst",
        "required_version",
        "dependency_type"
    )
)

edge_count = dependency_edges.count()

print(
    "Dependency edge count:",
    edge_count
)


# ============================================================
# 4. WRITE NPM PROCESSED DATA
# ============================================================

print("")
print("Writing package_nodes...")

package_nodes.write \
    .mode("overwrite") \
    .parquet(
        f"{OUT}/package_nodes"
    )

print("Writing dependency_edges...")

dependency_edges.write \
    .mode("overwrite") \
    .parquet(
        f"{OUT}/dependency_edges"
    )


# ============================================================
# 5. CREATE GITHUB REPOSITORY DIMENSION
# ============================================================

print("")
print("Creating GitHub repository dimension...")

repo_dim = (
    package_nodes

    .filter(
        F.col("repository_key").isNotNull()
    )

    .filter(
        F.col(
            "repository_key"
        ).startswith(
            "github.com/"
        )
    )

    .select(
        "repository_key"
    )

    .dropDuplicates()
)

repo_count = repo_dim.count()

print(
    "Unique GitHub repositories:",
    repo_count
)


# ============================================================
# 6. READ GITHUB RAW DATA
# ============================================================

print("")
print("========================================")
print("READING GITHUB RAW JSONL")
print("========================================")

github_raw = spark.read.text(
    RAW_GITHUB
)


# ============================================================
# 7. EXTRACT GITHUB REPOSITORY
# ============================================================

repo_json = F.get_json_object(
    "value",
    "$.repo"
)

repo_name = F.get_json_object(
    repo_json,
    "$.name"
)

github_repository_key = normalize_repo(
    F.concat(
        F.lit("github.com/"),
        repo_name
    )
)


# ============================================================
# 8. EXTRACT ONLY REQUIRED GITHUB FIELDS
# ============================================================

print("Extracting GitHub event fields...")

github_base = (
    github_raw

    .select(

        F.get_json_object(
            "value",
            "$.id"
        ).alias(
            "event_id"
        ),

        F.get_json_object(
            "value",
            "$.type"
        ).alias(
            "event_type"
        ),

        F.get_json_object(
            "value",
            "$.created_at"
        ).alias(
            "created_at"
        ),

        F.get_json_object(
            "value",
            "$.actor.login"
        ).alias(
            "actor_login"
        ),

        repo_name.alias(
            "repo_name"
        ),

        github_repository_key.alias(
            "repository_key"
        ),

        F.get_json_object(
            "value",
            "$.payload.action"
        ).alias(
            "action"
        ),

        F.get_json_object(
            "value",
            "$.payload.size"
        )
        .cast("long")
        .alias(
            "push_size"
        ),

        F.get_json_object(
            "value",
            "$.payload.distinct_size"
        )
        .cast("long")
        .alias(
            "push_distinct_size"
        ),

        F.get_json_object(
            "value",
            "$.payload.issue.number"
        )
        .cast("long")
        .alias(
            "issue_number"
        ),

        F.get_json_object(
            "value",
            "$.payload.issue.created_at"
        ).alias(
            "issue_created_at"
        ),

        F.get_json_object(
            "value",
            "$.payload.pull_request.number"
        )
        .cast("long")
        .alias(
            "pr_number"
        ),

        F.get_json_object(
            "value",
            "$.payload.pull_request.merged"
        ).alias(
            "pr_merged_raw"
        ),

        F.get_json_object(
            "value",
            "$.payload.pull_request.created_at"
        ).alias(
            "pr_created_at"
        ),

        F.get_json_object(
            "value",
            "$.payload.pull_request.merged_at"
        ).alias(
            "pr_merged_at"
        )
    )
)


# ============================================================
# 9. FILTER TO OUR RELEVANT REPOSITORIES
# ============================================================

print("")
print("Filtering GitHub events to relevant repositories...")

github_filtered = (
    github_base

    .join(
        F.broadcast(repo_dim),
        on="repository_key",
        how="left_semi"
    )

    .withColumn(
        "pr_merged",
        F.when(
            F.lower(
                F.col("pr_merged_raw")
            ) == "true",
            F.lit(True)
        )
        .when(
            F.lower(
                F.col("pr_merged_raw")
            ) == "false",
            F.lit(False)
        )
        .otherwise(
            F.lit(None).cast("boolean")
        )
    )

    .withColumn(
        "event_date",
        F.to_date(
            F.col("created_at")
        )
    )

    .drop(
        "pr_merged_raw"
    )
)


# ============================================================
# 10. CONTROL OUTPUT PARTITIONS
# ============================================================

print("")
print("Preparing filtered GitHub data for storage...")

github_filtered = github_filtered.repartition(
    8,
    "event_date"
)


# ============================================================
# 11. WRITE FILTERED GITHUB EVENTS
# ============================================================

print("")
print("Writing filtered GitHub events...")

github_filtered.write \
    .mode("overwrite") \
    .partitionBy(
        "event_date"
    ) \
    .parquet(
        f"{OUT}/github_filtered_events"
    )

print("")
print("GitHub filtering completed.")


# ============================================================
# 12. READ FILTERED GITHUB PARQUET
# ============================================================

print("")
print("Reading filtered GitHub Parquet...")

filtered = spark.read.parquet(
    f"{OUT}/github_filtered_events"
)


# ============================================================
# 13. GITHUB REPOSITORY ACTIVITY
# ============================================================

print("")
print("Calculating GitHub repository activity...")

repo_activity = (
    filtered

    .groupBy(
        "repository_key"
    )

    .agg(

        F.count("*").alias(
            "event_count"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "event_type"
                ) == "PushEvent",
                F.col(
                    "event_id"
                )
            )
        ).alias(
            "push_event_count"
        ),

        F.coalesce(
            F.sum(
                F.when(
                    F.col(
                        "event_type"
                    ) == "PushEvent",

                    F.coalesce(
                        F.col(
                            "push_size"
                        ),
                        F.lit(0)
                    )
                )
            ),
            F.lit(0)
        ).alias(
            "commit_activity"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "event_type"
                ) == "PushEvent",

                F.col(
                    "actor_login"
                )
            )
        ).alias(
            "active_contributors"
        ),

        F.min(
            "created_at"
        ).alias(
            "first_event_at"
        ),

        F.max(
            "created_at"
        ).alias(
            "last_event_at"
        )
    )
)

repo_activity.write \
    .mode("overwrite") \
    .parquet(
        f"{OUT}/github_repo_activity"
    )


# ============================================================
# 14. GITHUB CONTRIBUTORS
# ============================================================

print("")
print("Calculating GitHub contributors...")

contributors = (
    filtered

    .filter(
        F.col(
            "actor_login"
        ).isNotNull()
    )

    .groupBy(
        "repository_key",
        "actor_login"
    )

    .agg(

        F.count("*").alias(
            "event_count"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "event_type"
                ) == "PushEvent",

                F.col(
                    "event_id"
                )
            )
        ).alias(
            "push_events"
        )
    )
)

contributors.write \
    .mode("overwrite") \
    .parquet(
        f"{OUT}/github_contributors"
    )


# ============================================================
# 15. BUS FACTOR
# ============================================================

print("")
print("Calculating bus factor...")

pushes = (
    filtered
    .filter(
        (F.col("event_type") == "PushEvent")
        &
        F.col("actor_login").isNotNull()
    )
    .groupBy(
        "repository_key",
        "actor_login"
    )
    .agg(
        F.count("*").alias("push_count")
    )
)

partition_window = Window.partitionBy(
    "repository_key"
)

rank_window = (
    partition_window
    .orderBy(
        F.desc("push_count"),
        F.asc("actor_login")
    )
)

ranked = (
    pushes
    .withColumn(
        "total_pushes",
        F.sum("push_count").over(
            partition_window
        )
    )
    .withColumn(
        "contributor_rank",
        F.row_number().over(
            rank_window
        )
    )
    .withColumn(
        "cumulative_pushes",
        F.sum("push_count").over(
            rank_window.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )
    )
)

bus_factor = (
    ranked
    .filter(
        (
            F.col("cumulative_pushes")
            /
            F.when(
                F.col("total_pushes") == 0,
                F.lit(1)
            ).otherwise(
                F.col("total_pushes")
            )
        ) >= 0.5
    )
    .groupBy(
        "repository_key"
    )
    .agg(
        F.min(
            "contributor_rank"
        ).alias(
            "bus_factor"
        )
    )
)

bus_factor.write \
    .mode("overwrite") \
    .parquet(
        f"{OUT}/github_bus_factor"
    )