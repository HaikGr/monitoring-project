"""S3 upload helper.

This MUST be its own file — main.py does `from s3_import import upload_file_to_s3`.
In your current code this was pasted into the middle of the Postgres module,
which is why the import path was fragile.

Drop this file next to main.py in BOTH apps.
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "chat-exports").strip("/")
S3_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

_s3_client = boto3.client("s3", region_name=S3_REGION) if S3_REGION else boto3.client("s3")


def upload_file_to_s3(local_file_path: Path) -> str:
    """Upload a local file to S3 and return the object key."""
    path = Path(local_file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    key = f"{S3_PREFIX}/{path.name}" if S3_PREFIX else path.name

    try:
        _s3_client.upload_file(
            str(path),
            S3_BUCKET,
            key,
            ExtraArgs={"ContentType": "text/csv"},
        )
    except (ClientError, BotoCoreError, OSError) as exc:
        raise RuntimeError(
            f"Failed to upload {path.name} to s3://{S3_BUCKET}/{key}: {exc}"
        ) from exc

    print(f"Uploaded to s3://{S3_BUCKET}/{key}", flush=True)
    return key