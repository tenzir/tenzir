"""Verify the S3 fixture starts correctly and contains the test bucket."""

import os

import boto3


def main() -> None:
    """List buckets through the LocalStack S3 endpoint."""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['S3_ENDPOINT']}",
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    buckets = {bucket["Name"] for bucket in client.list_buckets()["Buckets"]}
    if os.environ["S3_BUCKET"] not in buckets:
        raise RuntimeError("LocalStack did not create the test bucket")
    print("s3-ready")


if __name__ == "__main__":
    main()
