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
    seen = [
        event["message"]
        for event in response["events"]
        if event["message"].startswith("cw-")
    ]
    print(f"all_written: {set(seen) >= {'cw-one', 'cw-two'}}")


if __name__ == "__main__":
    main()
