import boto3
import asyncio
import logging
import sys
import os

QUEUE_URL = os.getenv("QUEUE_URL")
AWS_REGION = os.getenv("AWS_REGION")

logging.getLogger(__name__).setLevel(logging.DEBUG)
sqs = boto3.client("sqs", region_name=AWS_REGION)


async def vast(*cmd):
    """A very basic helper to run vast commands"""
    return await asyncio.create_subprocess_exec(
        "vast",
        *cmd,
        stdin=None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


def check_code(proc, stderr):
    """Raise exception if proc has non-zero exit code"""
    if proc.returncode != 0:
        logging.error(stderr.decode())
        raise sys.exit(f"Vast with code {proc.returncode}")


async def matcher_list():
    proc = await vast("matcher", "list")
    stdout, stderr = await proc.communicate()
    check_code(proc, stderr)
    matchers_nl = stdout.decode().strip()
    if matchers_nl == "":
        sys.exit("No matcher to attach")
    matchers_list = []
    for line in matchers_nl.split("\n"):
        matchers_list.append(line.split(" ")[0])
    return ",".join(matchers_list)


async def matcher_attach(matchers):
    """Attach to matcher and forward events to SQS"""
    proc = await vast("matcher", "attach", "json", matchers)
    while not proc.stdout.at_eof():
        line = (await proc.stdout.readline()).decode()
        logging.debug(line)
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=line)
    _, stderr = await proc.communicate()
    check_code(proc, stderr)


async def main():
    matchers = await matcher_list()
    logging.info(f"Attaching to matchers: {matchers}")
    await matcher_attach(matchers)


if __name__ == "__main__":
    asyncio.run(main())
