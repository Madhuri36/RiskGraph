from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

OUT = "s3a://riskgraph/processed"

spark = (
    SparkSession.builder
    .appName("RiskGraph-Risk-Analysis")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://rustfs:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "riskgraph",
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "riskgraph_local_password",
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config(
        "spark.sql.adaptive.enabled",
        "true",
    )
    .config(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true",
    )
    .config(
        "spark.sql.shuffle.partitions",
        "8",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("========================================")
print("READING GRAPH METRICS")
print("========================================")

metrics = (
    spark.read.parquet(
        f"{OUT}/graph_metrics"
    )
    .filter(
        F.col("is_observed_package") == True
    )
)

package_count = metrics.count()

print(
    "Observed packages:",
    package_count,
)


print("========================================")
print("CHECKING INPUT DATA")
print("========================================")

metrics.select(
    "id",
    "in_degree",
    "out_degree",
    "pagerank",
    "betweenness_sampled",
    "kcore",
    "event_count",
    "active_contributors",
    "commit_count",
    "bus_factor",
    "avg_issue_first_response_hours",
    "last_observed_event",
).show(
    5,
    truncate=False,
)


print("========================================")
print("CALCULATING CENTRALITY RANKS")
print("========================================")

w_pagerank = Window.orderBy(
    F.col("pagerank")
)

w_in_degree = Window.orderBy(
    F.col("in_degree")
)

w_betweenness = Window.orderBy(
    F.col("betweenness_sampled")
)

w_kcore = Window.orderBy(
    F.col("kcore")
)


metrics = (
    metrics

    .withColumn(
        "pagerank_rank",
        F.percent_rank().over(
            w_pagerank
        ),
    )

    .withColumn(
        "in_degree_rank",
        F.percent_rank().over(
            w_in_degree
        ),
    )

    .withColumn(
        "betweenness_rank",
        F.percent_rank().over(
            w_betweenness
        ),
    )

    .withColumn(
        "kcore_rank",
        F.percent_rank().over(
            w_kcore
        ),
    )
)


print("Centrality ranks calculated.")


print("========================================")
print("CALCULATING CENTRALITY RISK")
print("========================================")

centrality_risk = (
    0.35 * F.col(
        "pagerank_rank"
    )
    +
    0.30 * F.col(
        "in_degree_rank"
    )
    +
    0.20 * F.col(
        "betweenness_rank"
    )
    +
    0.15 * F.col(
        "kcore_rank"
    )
)

metrics = metrics.withColumn(
    "centrality_risk",
    centrality_risk,
)


print("Centrality risk calculated.")


print("========================================")
print("CHECKING GITHUB OBSERVATIONS")
print("========================================")

metrics = metrics.withColumn(
    "has_github_observation",
    (
        (F.coalesce(
            F.col("event_count"),
            F.lit(0),
        ) > 0)
        |
        (F.coalesce(
            F.col("active_contributors"),
            F.lit(0),
        ) > 0)
        |
        (F.coalesce(
            F.col("commit_count"),
            F.lit(0),
        ) > 0)
        |
        F.col("last_observed_event").isNotNull()
    ),
)

observed_github_count = (
    metrics
    .filter(
        F.col(
            "has_github_observation"
        ) == True
    )
    .count()
)

print(
    "Packages with GitHub observations:",
    observed_github_count,
)

print(
    "Packages without GitHub observations:",
    package_count
    - observed_github_count,
)


print("========================================")
print("CALCULATING MAINTENANCE ACTIVITY RANKS")
print("========================================")

w_contributors = Window.orderBy(
    F.coalesce(
        F.col("active_contributors"),
        F.lit(0),
    )
)

w_commits = Window.orderBy(
    F.log1p(
        F.coalesce(
            F.col("commit_count"),
            F.lit(0),
        )
    )
)

w_events = Window.orderBy(
    F.log1p(
        F.coalesce(
            F.col("event_count"),
            F.lit(0),
        )
    )
)


metrics = (
    metrics

    .withColumn(
        "contributor_activity_rank",
        F.percent_rank().over(
            w_contributors
        ),
    )

    .withColumn(
        "commit_activity_rank",
        F.percent_rank().over(
            w_commits
        ),
    )

    .withColumn(
        "github_activity_rank",
        F.percent_rank().over(
            w_events
        ),
    )
)


print(
    "Maintenance activity ranks calculated."
)


print("========================================")
print("CALCULATING BUS FACTOR RISK")
print("========================================")

metrics = metrics.withColumn(
    "bus_factor_risk",
    F.when(
        ~F.col(
            "has_github_observation"
        ),
        F.lit(None).cast("double"),
    )
    .when(
        F.col("bus_factor").isNull()
        |
        (F.col("bus_factor") <= 0),
        F.lit(1.0),
    )
    .otherwise(
        F.least(
            F.lit(1.0),
            F.lit(1.0)
            / F.col("bus_factor"),
        )
    ),
)


print(
    "Bus factor risk calculated."
)


print("========================================")
print("CALCULATING CONTRIBUTOR RISK")
print("========================================")

metrics = metrics.withColumn(
    "contributor_risk",
    F.when(
        ~F.col(
            "has_github_observation"
        ),
        F.lit(None).cast("double"),
    )
    .otherwise(
        1.0
        -
        F.col(
            "contributor_activity_rank"
        )
    ),
)


print("Contributor risk calculated.")


print("========================================")
print("CALCULATING COMMIT ACTIVITY RISK")
print("========================================")

metrics = metrics.withColumn(
    "commit_activity_risk",
    F.when(
        ~F.col(
            "has_github_observation"
        ),
        F.lit(None).cast("double"),
    )
    .otherwise(
        1.0
        -
        F.col(
            "commit_activity_rank"
        )
    ),
)


print(
    "Commit activity risk calculated."
)


print("========================================")
print("CALCULATING GITHUB ACTIVITY RISK")
print("========================================")

metrics = metrics.withColumn(
    "github_activity_risk",
    F.when(
        ~F.col(
            "has_github_observation"
        ),
        F.lit(None).cast("double"),
    )
    .otherwise(
        1.0
        -
        F.col(
            "github_activity_rank"
        )
    ),
)


print(
    "GitHub activity risk calculated."
)


print("========================================")
print("CALCULATING RECENCY RISK")
print("========================================")

snapshot = (
    metrics
    .agg(
        F.max(
            "last_observed_event"
        ).alias(
            "snapshot_end"
        )
    )
    .collect()[0]["snapshot_end"]
)

if snapshot is None:

    print(
        "No GitHub timestamps available."
    )

    metrics = metrics.withColumn(
        "days_since_last_event",
        F.lit(None).cast("int"),
    )

    metrics = metrics.withColumn(
        "recency_risk",
        F.lit(None).cast("double"),
    )

else:

    print(
        "Observation snapshot:",
        snapshot,
    )

    metrics = metrics.withColumn(
        "days_since_last_event",
        F.when(
            F.col(
                "last_observed_event"
            ).isNull(),
            F.lit(None).cast("int"),
        )
        .otherwise(
            F.datediff(
                F.to_date(
                    F.lit(snapshot)
                ),
                F.to_date(
                    F.col(
                        "last_observed_event"
                    )
                ),
            )
        ),
    )

    metrics = metrics.withColumn(
        "recency_risk",
        F.when(
            ~F.col(
                "has_github_observation"
            ),
            F.lit(None).cast("double"),
        )
        .otherwise(
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    F.col(
                        "days_since_last_event"
                    )
                    / F.lit(7.0),
                ),
            )
        ),
    )


print(
    "Recency risk calculated."
)


print("========================================")
print("BUILDING MAINTENANCE RISK")
print("========================================")

maintenance_risk_observed = (
    0.30 * F.col(
        "contributor_risk"
    )
    +
    0.30 * F.col(
        "bus_factor_risk"
    )
    +
    0.25 * F.col(
        "commit_activity_risk"
    )
    +
    0.15 * F.col(
        "recency_risk"
    )
)

metrics = metrics.withColumn(
    "maintenance_risk_observed",
    maintenance_risk_observed,
)


print(
    "Maintenance risk calculated."
)


print("========================================")
print("HANDLING MISSING GITHUB OBSERVATIONS")
print("========================================")

# A package without observed GitHub activity
# should not automatically receive maximum
# maintenance risk.
#
# We use the median maintenance risk of packages
# with observations as a neutral fallback.

median_maintenance = (
    metrics
    .filter(
        F.col(
            "has_github_observation"
        ) == True
    )
    .approxQuantile(
        "maintenance_risk_observed",
        [0.50],
        0.01,
    )
)

if median_maintenance:

    neutral_maintenance_risk = float(
        median_maintenance[0]
    )

else:

    neutral_maintenance_risk = 0.50


print(
    "Neutral maintenance risk:",
    neutral_maintenance_risk,
)

metrics = metrics.withColumn(
    "maintenance_risk",
    F.when(
        F.col(
            "has_github_observation"
        ) == True,
        F.col(
            "maintenance_risk_observed"
        ),
    )
    .otherwise(
        F.lit(
            neutral_maintenance_risk
        )
    ),
)


print("========================================")
print("CALCULATING FINAL RISK SCORE")
print("========================================")

# 65% dependency/structural risk
# 35% maintenance risk

metrics = metrics.withColumn(
    "final_risk_score",
    (
        0.65
        *
        F.col(
            "centrality_risk"
        )
    )
    +
    (
        0.35
        *
        F.col(
            "maintenance_risk"
        )
    ),
)


print(
    "Final risk score calculated."
)


print("========================================")
print("ASSIGNING RISK LEVEL")
print("========================================")

risk_scores = (
    metrics
    .withColumn(
        "risk_level",
        F.when(
            F.col(
                "final_risk_score"
            ) >= 0.75,
            "HIGH",
        )
        .when(
            F.col(
                "final_risk_score"
            ) >= 0.50,
            "MEDIUM",
        )
        .otherwise(
            "LOW"
        ),
    )
)


print("Risk levels assigned.")


print("========================================")
print("SELECTING FINAL OUTPUT COLUMNS")
print("========================================")

risk_scores = risk_scores.select(
    "id",
    "version",
    "description",
    "repository_key",

    "in_degree",
    "out_degree",
    "pagerank",
    "betweenness_sampled",
    "kcore",
    "community_id",

    "event_count",
    "active_contributors",
    "commit_count",
    "bus_factor",
    "avg_issue_first_response_hours",
    "last_observed_event",

    "has_github_observation",
    "days_since_last_event",

    "pagerank_rank",
    "in_degree_rank",
    "betweenness_rank",
    "kcore_rank",

    "centrality_risk",

    "contributor_activity_rank",
    "commit_activity_rank",
    "github_activity_rank",

    "contributor_risk",
    "bus_factor_risk",
    "commit_activity_risk",
    "github_activity_risk",
    "recency_risk",

    "maintenance_risk_observed",
    "maintenance_risk",

    "final_risk_score",
    "risk_level",
)


print("========================================")
print("WRITING RISK SCORES")
print("========================================")

risk_scores.write.mode(
    "overwrite"
).parquet(
    f"{OUT}/risk_scores"
)

print(
    "Risk scores written to:",
    f"{OUT}/risk_scores",
)


print("========================================")
print("RISK DISTRIBUTION")
print("========================================")

(
    risk_scores
    .groupBy(
        "risk_level"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show()
)


print("========================================")
print("TOP 30 HIGH-RISK PACKAGES")
print("========================================")

(
    risk_scores
    .orderBy(
        F.desc(
            "final_risk_score"
        )
    )
    .select(
        "id",
        "repository_key",
        "in_degree",
        "pagerank",
        "betweenness_sampled",
        "kcore",
        "event_count",
        "active_contributors",
        "commit_count",
        "bus_factor",
        "centrality_risk",
        "maintenance_risk",
        "final_risk_score",
        "risk_level",
    )
    .show(
        30,
        truncate=False,
    )
)


print("========================================")
print("RISK ANALYSIS COMPLETE")
print("========================================")

print(
    "Observed packages:",
    package_count,
)

print(
    "Output:",
    f"{OUT}/risk_scores",
)

print("========================================")

spark.stop()