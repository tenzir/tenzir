from typing import List, Tuple
from vast_invoke import pty_task, task, Context, Exit
import dynaconf
import time
import base64
import json
import io
from common import (
    AWS_REGION,
    COMMON_VALIDATORS,
    conf,
    TFDIR,
    AWS_REGION_VALIDATOR,
    auto_app_fmt,
    REPOROOT,
    DOCKERDIR,
    load_cmd,
    terraform_output,
    aws,
)


VALIDATORS = [
    *COMMON_VALIDATORS,
    AWS_REGION_VALIDATOR,
    dynaconf.Validator("VAST_CIDR", must_exist=True, ne=""),
    dynaconf.Validator("VAST_PEERED_VPC_ID", must_exist=True, ne=""),
    dynaconf.Validator("VAST_IMAGE", default="tenzir/vast"),
    dynaconf.Validator("VAST_VERSION"),  # usually resolved lazily
    dynaconf.Validator(
        "VAST_SERVER_STORAGE_TYPE", default="EFS", is_in=["EFS", "ATTACHED"]
    ),
]

CMD_HELP = {
    "cmd": "Bash commands to be executed. Can be either a plain command\
 (we recommend wrapping it with single quotes to avoid unexpected\
 interpolations), a local absolute file path (e.g file:///etc/mycommands)\
 or an s3 location (e.g s3://mybucket/key)",
    "env": "List of environment variables to be passed to the execution context,\
 name and values are separated by = (e.g --env BUCKET=mybucketname)",
}


##  Aliases
EXIT_CODE_VAST_SERVER_NOT_RUNNING = 8


## Helper functions


def VAST_VERSION(c: Context):
    """If VAST_VERSION not defined, use latest release"""
    if "VAST_VERSION" in conf():
        return conf()["VAST_VERSION"]
    version = c.run(
        "git describe --abbrev=0 --match='v[0-9]*' --exclude='*-rc*'", hide="out"
    ).stdout.strip()
    return version


def env(c: Context) -> dict:
    return {
        **conf(VALIDATORS),
        "VAST_VERSION": VAST_VERSION(c),
    }


def parse_env(env: List[str]) -> Tuple[str, str]:
    def split_name_val(name_val):
        env_split = name_val.split("=")
        if len(env_split) != 2:
            raise Exit(f"{name_val} should have exactly one '=' char", 1)
        return env_split[0], env_split[1]

    name_val_list = [split_name_val(v) for v in env]
    return {v[0]: v[1] for v in name_val_list}


## Tasks


@task
def fmt(c, fix=False):
    """Fix Terraform and Terragrunt formatting"""
    tf_fix = "" if fix else "--check"
    c.run(f"terraform fmt -recursive -diff {tf_fix}")
    tg_fix = "" if fix else "--terragrunt-check"
    c.run(f"terragrunt hclfmt {tg_fix}")


@task
def docker_login(c):
    """Login the Docker client to ECR"""
    token = aws("ecr").get_authorization_token()
    user_pass = (
        base64.b64decode(token["authorizationData"][0]["authorizationToken"])
        .decode()
        .split(":")
    )
    registry = token["authorizationData"][0]["proxyEndpoint"]
    c.run(
        f"docker login --username {user_pass[0]} --password-stdin {registry}",
        in_stream=io.StringIO(user_pass[1]),
    )


@task
def init_step(c, step):
    """Manually run terraform init on a specific step"""
    c.run(
        f"terragrunt init --terragrunt-working-dir {TFDIR}/{step}",
        env=env(c),
    )


@pty_task
def deploy_step(c, step, auto_approve=False):
    """Deploy only one step of the stack"""
    init_step(c, step)
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
        env=env(c),
    )


@pty_task
def deploy(c, auto_approve=False):
    """One liner build and deploy of the stack to AWS"""
    deploy_step(c, "core-1", auto_approve)
    deploy_step(c, "core-2", auto_approve)


@task(autoprint=True)
def current_image(c, repo_arn):
    """Get the current Lambda image URI. In case of failure, returns the error message instead of the URI."""
    try:
        tags = aws("ecr").list_tags_for_resource(resourceArn=repo_arn)["tags"]
    except Exception as e:
        return str(e)
    current = next(
        (tag["Value"] for tag in tags if tag["Key"] == "current"),
        "current-image-not-defined",
    )
    return current


@task(help={"type": "Can be either 'lambda' or 'fargate'"})
def deploy_image(c, type):
    """Build and push the image, fails if step 1 is not deployed"""
    image_url = terraform_output(c, "core-1", f"vast_{type}_repository_url")
    repo_arn = terraform_output(c, "core-1", f"vast_{type}_repository_arn")
    # get the digest of the current image
    try:
        current_img = current_image(c, repo_arn)
        c.run(f"docker pull {current_img}")
        old_digest = c.run(
            f"docker inspect --format='{{{{.RepoDigests}}}}' {current_img}",
            hide="out",
        ).stdout
    except:
        old_digest = "current-image-not-found"
    # build the image and get the new digest
    base_image = env(c)["VAST_IMAGE"]
    image_tag = int(time.time())
    version = VAST_VERSION(c)
    if version == "build":
        c.run(f"docker build -t {base_image}:{image_tag} {REPOROOT}")
        version = image_tag
    c.run(
        f"""docker build \
            --build-arg VAST_VERSION={version} \
            --build-arg BASE_IMAGE={base_image} \
            -f {DOCKERDIR}/{type}.Dockerfile \
            -t {image_url}:{image_tag} \
            {DOCKERDIR}"""
    )
    new_digest = c.run(
        f"docker inspect --format='{{{{.RepoDigests}}}}' {image_url}:{image_tag}",
        hide="out",
    ).stdout
    # compare old an new digests
    if old_digest == new_digest:
        print("Docker image didn't change, skipping push")
        return
    # if a change occured, push and tag the new image as current
    c.run(f"docker push {image_url}:{image_tag}")
    aws("ecr").tag_resource(
        resourceArn=repo_arn,
        tags=[{"Key": "current", "Value": f"{image_url}:{image_tag}"}],
    )


@task(autoprint=True)
def get_vast_server(c, max_wait_time_sec=0):
    """Get the task id of the VAST server. If no server is running, it waits
    until max_wait_time_sec for a new server to be started."""
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "core-2", "vast_task_family")
    start_time = time.time()
    while True:
        task_res = aws("ecs").list_tasks(family=family, cluster=cluster)
        nb_vast_tasks = len(task_res["taskArns"])
        if nb_vast_tasks == 1:
            task_id = task_res["taskArns"][0].split("/")[-1]
            return task_id
        if nb_vast_tasks > 1:
            raise Exit(f"{nb_vast_tasks} VAST servers running", 1)
        if nb_vast_tasks == 0 and time.time() - start_time > max_wait_time_sec:
            raise Exit("No VAST server running", EXIT_CODE_VAST_SERVER_NOT_RUNNING)
        time.sleep(1)


@task
def start_vast_server(c):
    """Start the VAST server instance as an AWS Fargate task. Noop if a VAST server is already running"""
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    service_name = terraform_output(c, "core-2", "vast_service_name")
    aws("ecs").update_service(cluster=cluster, service=service_name, desiredCount=1)
    task_id = get_vast_server(c, max_wait_time_sec=120)
    print(f"Started task {task_id}")


def stop_vast_task(c):
    "Stop the current running VAST Fargate Task"
    task_id = get_vast_server(c)
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    aws("ecs").stop_task(task=task_id, cluster=cluster)
    return task_id


@task
def stop_vast_server(c):
    """Stop the VAST server instance"""
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    service_name = terraform_output(c, "core-2", "vast_service_name")
    aws("ecs").update_service(cluster=cluster, service=service_name, desiredCount=0)
    task_id = stop_vast_task(c)
    print(f"Stopped task {task_id}")


@task
def restart_vast_server(c):
    """Stop the running VAST server Fargate task, the service starts a new one"""
    task_id = stop_vast_task(c)
    print(f"Stopped task {task_id}")
    # 120 seconds corresponding to the task stopTimeout grace period
    # + 120 seconds for the new task to start
    task_id = get_vast_server(c, max_wait_time_sec=240)
    print(f"Started task {task_id}")


@task(autoprint=True)
def list_all_tasks(c):
    """List the ids of all tasks running on the ECS cluster"""
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    task_res = aws("ecs").list_tasks(cluster=cluster)
    task_ids = [task.split("/")[-1] for task in task_res["taskArns"]]
    return task_ids


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
def run_lambda(c, cmd, env=[], json_output=False):
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
def execute_command(c, cmd="/bin/bash", env=[]):
    """Run ad-hoc or interactive commands from the VAST server Fargate task"""
    task_id = get_vast_server(c)
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    # if we are not running the default interactive shell, encode the command to avoid escaping issues
    if cmd != "/bin/bash":
        cmd_bytes = load_cmd(cmd)
        parse_env(env)  # validate format of env list items
        cmd = f"/bin/bash -c 'echo {base64.b64encode(cmd_bytes).decode()} | base64 -d | {' '.join(env)} /bin/bash'"
    # we use the CLI here because boto does not know how to use the session-manager-plugin
    # unbuffer (expect package) helps ensuring a clean session closing when there is no PTY
    c.run(
        f"""unbuffer aws ecs execute-command \
		--cluster {cluster} \
		--task {task_id} \
		--interactive \
		--command "{cmd}" \
        --region {AWS_REGION()} """,
    )


@pty_task
def destroy_step(c, step, auto_approve=False):
    """Destroy resources of the specified step. Resources depending on it should be cleaned up first."""
    init_step(c, step)
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
        env=env(c),
    )


@pty_task
def destroy(c, auto_approve=False):
    """Tear down the entire terraform stack"""
    try:
        stop_vast_server(c)
    except Exception as e:
        print(str(e))
        print("Failed to stop tasks. Continuing destruction...")
    c.run(
        f"terragrunt run-all destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}",
        env=env(c),
    )
