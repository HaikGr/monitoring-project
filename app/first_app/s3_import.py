import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "chat-exports").strip("/")

s3_client = boto3.client("s3")


def upload_file_to_s3(local_file_path: Path) -> str:
    path = Path(local_file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    key = f"{S3_PREFIX}/{path.name}" if S3_PREFIX else path.name

    try:
        s3_client.upload_file(str(path), S3_BUCKET, key)
    except (ClientError, OSError) as exc:
        raise RuntimeError(
            f"Failed to upload {path.name} to s3://{S3_BUCKET}/{key}: {exc}"
        ) from exc

    print(f"Uploaded to s3://{S3_BUCKET}/{key}", flush=True)
    return key
