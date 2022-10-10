"""Manage and interact with the VAST server"""
from typing import Tuple
from vast_invoke import Context, pty_task, task, Exit
import base64
import json
from common import (
    AWS_REGION,
    FargateService,
    load_cmd,
    parse_env,
    terraform_output,
    aws,
)


CMD_HELP = {
    "cmd": "Bash commands to be executed. Can be either a plain command\
 (we recommend wrapping it with single quotes to avoid unexpected\
 interpolations), a local absolute file path (e.g file:///etc/mycommands)\
 or an s3 location (e.g s3://mybucket/key)",
    "env": "List of environment variables to be passed to the execution context,\
 name and values are separated by = (e.g --env BUCKET=mybucketname)",
}


## Tasks


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "core-2", "vast_task_family")
    service_name = terraform_output(c, "core-2", "vast_server_service_name")
    return (cluster, service_name, family)


@task
def server_status(c):
    """Get the status of the VAST server"""
    print(FargateService(*service_outputs(c)).service_status())


@task
def start_server(c):
    """Start the VAST server instance as an AWS Fargate task. Noop if a VAST server is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop_server(c):
    """Stop the VAST server instance"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart_server(c):
    """Stop the running VAST server Fargate task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


def print_lambda_output(json_response: str, json_output: bool):
    if json_output:
        print(json_response)
    else:
        response = json.loads(json_response)
        print("PARSED COMMAND:")
        print(response["parsed_cmd"])
        print("\nENV:")
        print(response["env"])
        print("\nSTDOUT:")
        print(response["stdout"])
        print("\nSTDERR:")
        print(response["stderr"])
        print("\nRETURN CODE:")
        print(response["returncode"])


@task(help=CMD_HELP, iterable=["env"])
def lambda_client(c, cmd, env=[], json_output=False):
    """Run ad-hoc VAST client commands from AWS Lambda

    Prints the inputs (command / environment) and outputs (stdout, stderr, exit
    code) of the executed function to stdout."""
    lambda_name = terraform_output(c, "core-2", "vast_lambda_name")
    cmd_b64 = base64.b64encode(load_cmd(cmd)).decode()
    lambda_res = aws("lambda").invoke(
        FunctionName=lambda_name,
        Payload=json.dumps({"cmd": cmd_b64, "env": parse_env(env)}).encode(),
        InvocationType="RequestResponse",
    )
    resp_payload = lambda_res["Payload"].read().decode()
    if "FunctionError" in lambda_res:
        # For command errors (the most likely ones), display the same object as
        # for successful results. Otherwise display the raw error payload.
        mess = resp_payload
        try:
            json_payload = json.loads(resp_payload)
            if json_payload["errorType"] == "CommandException":
                # CommandException is JSON encoded
                print_lambda_output(json_payload["errorMessage"], json_output)
                mess = ""
        except Exception:
            pass
        raise Exit(message=mess, code=1)
    print_lambda_output(resp_payload, json_output)


@pty_task(help=CMD_HELP, iterable=["env"])
def server_execute(c, cmd="/bin/bash", env=[]):
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
