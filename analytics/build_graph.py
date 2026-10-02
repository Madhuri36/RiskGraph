"""Starter Spark job: consume npm Kafka records, then construct package vertices/edges."""
import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, functions as F, types as T

consumer = KafkaConsumer(
    "npm-packages",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=10000,
)
records = [message.value for message in consumer]
consumer.close()

spark = (SparkSession.builder.appName("RiskGraph-BuildDependencyGraph")
         .master("local[*]").getOrCreate())
schema = T.StructType([
    T.StructField("name", T.StringType()),
    T.StructField("version", T.StringType()),
    T.StructField("dependencies", T.MapType(T.StringType(), T.StringType())),
    T.StructField("collected_at", T.LongType()),
    T.StructField("source", T.StringType()),
])
if not records:
    print("No npm records found. Run ingestion/npm_producer.py first.")
    spark.stop()
    raise SystemExit(0)

packages = (spark.createDataFrame(records, schema=schema)
            .filter(F.col("name").isNotNull()).dropDuplicates(["name"]))
vertices = packages.select(F.col("name").alias("id"), "version", "collected_at")
edges = (packages.select("name", F.explode_outer("dependencies").alias("dependency", "range"))
         .filter(F.col("dependency").isNotNull())
         .select(F.col("name").alias("src"), F.col("dependency").alias("dst"), "range"))
vertices.write.mode("overwrite").parquet("data/vertices")
edges.write.mode("overwrite").parquet("data/edges")
print("Vertices:", vertices.count(), "Edges:", edges.count())
spark.stop()
