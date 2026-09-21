import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv(
    "S3_PREFIX",
    "chat-exports",
)

s3_client = boto3.client("s3")


def upload_file_to_s3(
    local_file_path: Path,
) -> str:
    """
    Upload a local CSV file to S3.

    Returns:
        S3 object key.
    """

    local_file_path = Path(
        local_file_path
    )

    if not local_file_path.exists():
        raise FileNotFoundError(
            f"File not found: {local_file_path}"
        )

    s3_key = (
        f"{S3_PREFIX}/"
        f"{local_file_path.name}"
    )

    try:
        s3_client.upload_file(
            str(local_file_path),
            S3_BUCKET,
            s3_key,
        )

        print(
            "Successfully uploaded file to S3:"
            f" s3://{S3_BUCKET}/{s3_key}",
            flush=True,
        )

        return s3_key

    except ClientError as exc:
        print(
            f"S3 upload failed: {exc}",
            flush=True,
        )
        raise