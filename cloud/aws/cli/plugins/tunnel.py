import base64
from typing import Tuple
from vast_invoke import Context, pty_task, task
from common import (
    AWS_REGION,
    FargateService,
    load_cmd,
    parse_env,
    terraform_output,
)


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "tunnel", "tunnel_task_family")
    service_name = terraform_output(c, "tunnel", "tunnel_service_name")
    return (cluster, service_name, family)


@task
def status(c):
    """Get the status of the tunnel service"""
    print(FargateService(*service_outputs(c)).get_task_status())


@task
def start(c):
    """Start the tunnel as an AWS Fargate task. Noop if it is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop(c):
    """Stop the tunnel instance and service"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart(c):
    """Stop the running tunnel task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


@pty_task(iterable=["env"])
def execute_command(c, cmd="/bin/bash", env=[]):
    """Run ad-hoc or interactive commands from the VAST server Fargate task"""
    task_id = FargateService(*service_outputs(c)).get_task_id()
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    # if we are not running the default interactive shell, encode the command to avoid escaping issues
    buf = ""
    if cmd != "/bin/bash":
        cmd_bytes = load_cmd(cmd)
        parse_env(env)  # validate format of env list items
        cmd = f"/bin/bash -c 'echo {base64.b64encode(cmd_bytes).decode()} | base64 -d | {' '.join(env)} /bin/bash'"
        buf = "unbuffer"
    # we use the CLI here because boto does not know how to use the session-manager-plugin
    # unbuffer (expect package) helps ensuring a clean session closing when there is no PTY
    c.run(
        f"""{buf} aws ecs execute-command \
		--cluster {cluster} \
		--task {task_id} \
		--interactive \
		--command "{cmd}" \
        --region {AWS_REGION()} """,
    )
