
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="riskgraph",
    secret_key="riskgraph_local_password",
    secure=False
)

bucket = "riskgraph"

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Created bucket: {bucket}")
else:
    print(f"Bucket already exists: {bucket}")
