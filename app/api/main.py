import os
from fastapi import FastAPI
from minio import Minio
from minio.error import S3Error

app = FastAPI(title="RiskGraph API", version="0.1.0")
BUCKET = os.getenv("MINIO_BUCKET", "riskgraph")

@app.get("/")
def root():
    return {"project": "RiskGraph", "status": "running"}

@app.get("/health")
def health():
    return {"api": "ok"}

@app.get("/storage")
def storage_status():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "riskgraph"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "riskgraph_local_password"),
        secure=False,
    )
    try:
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
        return {"minio": "connected", "bucket": BUCKET}
    except S3Error as exc:
        return {"minio": "error", "detail": str(exc)}
