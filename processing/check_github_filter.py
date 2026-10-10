from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("RiskGraph-Check-GitHub-Filter")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    .config("spark.hadoop.fs.s3a.access.key", "riskgraph")
    .config("spark.hadoop.fs.s3a.secret.key", "riskgraph_local_password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(
    "s3a://riskgraph/processed/github_filtered_events"
)

print("========================================")
print("GITHUB FILTER DIAGNOSTIC")
print("========================================")

print("Total filtered events:", df.count())

print("\nEvents by type:")
df.groupBy("event_type").count().orderBy(
    F.desc("count")
).show(30, False)

print("\nEvents by repository:")
df.groupBy("repository_key").count().orderBy(
    F.desc("count")
).show(30, False)

print("\nEvents by date:")
df.groupBy("event_date").count().orderBy(
    "event_date"
).show(20, False)

print("\nDistinct repositories:", df.select(
    "repository_key"
).distinct().count())

print("\nDate range:")
df.select(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date")
).show()

print("========================================")

spark.stop()