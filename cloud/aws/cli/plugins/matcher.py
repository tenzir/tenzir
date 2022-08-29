"""This cloud plugin requires the VAST Pro image to be deployed"""

from typing import Tuple
from common import FargateService, aws, terraform_output
from vast_invoke import Context, task


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "matcher", "matcher_task_family")
    service_name = terraform_output(c, "matcher", "matcher_service_name")
    return (cluster, service_name, family)


@task
def client_status(c):
    """Get the status of the matcher client"""
    print(FargateService(*service_outputs(c)).get_task_status())


@task
def start_client(c):
    """Start the matcher client instance as an AWS Fargate task. Noop if the client is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop_client(c):
    """Stop the matcher client instance"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart_client(c):
    """Stop the running matcher client task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


@task
def attach(c):
    """Consume matched events from a queue alimented by the matcher client"""
    queue_url = terraform_output(c, "matcher", "matched_events_queue_url")
    queue = aws("sqs", resource=True).Queue(queue_url)
    while True:
        messages = queue.receive_messages(VisibilityTimeout=10, WaitTimeSeconds=20)
        for message in messages:
            print(message.body, flush=True)
            message.delete()
