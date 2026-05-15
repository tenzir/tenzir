# runner: python

from __future__ import annotations

import os

import boto3


def main() -> None:
    logs = boto3.client(
        "logs",
        endpoint_url=os.environ["AWS_ENDPOINT_URL_LOGS"],
        region_name=os.environ["AWS_REGION"],
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = logs.get_log_events(
        logGroupName=os.environ["LOCALSTACK_CLOUDWATCH_LOG_GROUP"],
        logStreamName=os.environ["LOCALSTACK_CLOUDWATCH_LOG_STREAM_WRITE"],
        startFromHead=True,
    )
    messages = {event["message"] for event in response["events"]}
    expected = {
        "cw-split-1",
        "cw-split-2",
        "cw-split-3",
        "cw-parallel-1",
        "cw-parallel-2",
        "cw-parallel-3",
        "cw-parallel-4",
        "cw-role",
        "cw-after-large",
    }
    print(f"runtime_writes_ok: {expected <= messages}")


if __name__ == "__main__":
    main()
