
from minio import Minio
from pathlib import Path

client = Minio(
    "localhost:9000",
    access_key="riskgraph",
    secret_key="riskgraph_local_password",
    secure=False,
)

bucket = "riskgraph"
prefix = "processed/"
output = Path("app/dashboard/data")

output.mkdir(parents=True, exist_ok=True)

total = 0

for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
    if not obj.object_name.endswith(".parquet"):
        continue

    relative = obj.object_name[len(prefix):]
    destination = output / relative
    destination.parent.mkdir(parents=True, exist_ok=True)

    client.fget_object(bucket, obj.object_name, str(destination))
    total += 1
    print(f"Downloaded: {obj.object_name} ({obj.size:,} bytes)")

print(f"\nDownloaded {total} Parquet files to {output.resolve()}")
