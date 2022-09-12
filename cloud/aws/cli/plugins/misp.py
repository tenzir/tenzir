import os
import pathlib
import stat
from typing import Tuple
from vast_invoke import Context, pty_task, task
import dynaconf
from common import (
    FargateService,
    aws,
    terraform_output,
)

VALIDATORS = [dynaconf.Validator("VAST_MISP_VERSION", default="v2.4.145a")]


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "misp", "misp_task_family")
    service_name = terraform_output(c, "misp", "misp_service_name")
    return (cluster, service_name, family)


@task
def status(c):
    """Get the status of the MISP service"""
    print(FargateService(*service_outputs(c)).service_status())


@task
def start(c):
    """Start the MISP as an AWS Fargate task. Noop if it is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop(c):
    """Stop the MISP instance and service"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart(c):
    """Stop the running MISP task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


def get_public_ip(c):
    """Resolve the public IP of the MISP task"""
    task = FargateService(*service_outputs(c)).describe_task()
    eni_attach = next(
        x for x in task["attachments"] if x["type"] == "ElasticNetworkInterface"
    )
    eni_id = next(
        x["value"] for x in eni_attach["details"] if x["name"] == "networkInterfaceId"
    )

    eni_desc = aws("ec2").describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    return eni_desc["NetworkInterfaces"][0]["Association"]["PublicIp"]


@pty_task
def tunnel(c, ui_port="8080", zmq_port="50000"):
    """Open an SSH tunnel to the MISP instance and forward the ports"""
    print("Getting public ip...")
    pub_ip = get_public_ip(c)

    print("Getting private key...")
    private_key = terraform_output(c, "misp", "ssh_tunneling_private_key")
    key_file = pathlib.Path.home() / ".ssh" / "tunneling"
    key_file.parent.mkdir(exist_ok=True, parents=True)
    with key_file.open("w") as f:
        f.write(private_key.replace("\r\n", "\n"))
    os.chmod(key_file, stat.S_IREAD | stat.S_IWRITE)

    cmd = (
        "sudo ssh "
        + '-o "StrictHostKeyChecking=no" '
        + "-i ~/.ssh/tunneling "
        + "-N "
        + "-p 2222 "
        + f"-L {ui_port}:localhost:8080 "
        + f"-L {zmq_port}:localhost:50000 "
        + f"tunneler@{pub_ip}"
    )
    print(f"Running tunneling (Ctrl+C to exit): {cmd}")
    c.run(cmd)
