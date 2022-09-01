from typing import Tuple
from vast_invoke import Context, pty_task, task, Exit
import dynaconf
import time
import base64
import json
import io
from common import (
    AWS_REGION,
    RESOURCEDIR,
    FargateService,
    active_modules,
    clean_modules,
    conf,
    TFDIR,
    auto_app_fmt,
    REPOROOT,
    DOCKERDIR,
    AWS_REGION_VALIDATOR,
    default_vast_version,
    load_cmd,
    parse_env,
    terraform_output,
    aws,
)

# Validate and provide defaults for the terraform state backend configuration
TF_BACKEND_VALIDATORS = [
    dynaconf.Validator("TF_STATE_BACKEND", default="local", is_in=["local", "cloud"]),
    dynaconf.Validator("TF_WORKSPACE_PREFIX", default=""),
    # if we use tf cloud as backend, the right variables must be configured
    dynaconf.Validator("TF_STATE_BACKEND", ne="cloud")
    | (
        dynaconf.Validator("TF_ORGANIZATION", must_exist=True, ne="")
        & dynaconf.Validator("TF_API_TOKEN", must_exist=True, ne="")
    ),
]


VALIDATORS = [
    *TF_BACKEND_VALIDATORS,
    AWS_REGION_VALIDATOR,
    dynaconf.Validator("VAST_CIDR", must_exist=True, ne=""),
    dynaconf.Validator("VAST_PEERED_VPC_ID", must_exist=True, ne=""),
    dynaconf.Validator("VAST_IMAGE", default="tenzir/vast"),
    dynaconf.Validator("VAST_VERSION", default=default_vast_version()),
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


def active_include_dirs(c: Context) -> str:
    """The --include-dir arguments for modules activated and core modules"""
    return " ".join([f"--terragrunt-include-dir={mod}" for mod in active_modules(c)])


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
    mods = active_modules(c)
    if step not in mods:
        raise Exit(f"Step {step} not part of the active modules {mods}")
    c.run(
        f"terragrunt init --terragrunt-working-dir {TFDIR}/{step}",
    )


@task(help={"clean": clean_modules.__doc__})
def init(c, clean=False):
    """Manually run terraform init on all steps"""
    if clean:
        clean_modules()
    c.run(
        f"terragrunt run-all init {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
    )


@pty_task
def deploy_step(c, step, auto_approve=False):
    """Deploy only one step of the stack"""
    init_step(c, step)
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
    )


@pty_task
def deploy(c, auto_approve=False):
    """One liner build and deploy of the stack to AWS with all the modules associated with active plugins"""
    c.run(
        f"terragrunt run-all apply {auto_app_fmt(auto_approve)} {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
    )


@task(autoprint=True)
def current_image(c, repo_arn, type):
    """Get the current Lambda image URI. In case of failure, returns the error message instead of the URI."""
    try:
        tags = aws("ecr").list_tags_for_resource(resourceArn=repo_arn)["tags"]
    except Exception as e:
        return str(e)
    current = next(
        (tag["Value"] for tag in tags if tag["Key"] == f"current-{type}"),
        "current-image-not-defined",
    )
    return current


@task(help={"type": "Can be either 'lambda' or 'fargate'"})
def deploy_image(c, type):
    """Build and push the image, fails if core-1 is not deployed"""
    image_url = terraform_output(c, "core-1", f"vast_repository_url")
    repo_arn = terraform_output(c, "core-1", f"vast_repository_arn")
    # get the digest of the current image
    try:
        current_img = current_image(c, repo_arn, type)
        c.run(f"docker pull {current_img}")
        old_digest = c.run(
            f"docker inspect --format='{{{{.RepoDigests}}}}' {current_img}",
            hide="out",
        ).stdout
    except:
        old_digest = "current-image-not-found"
    # build the image and get the new digest
    base_image = conf(VALIDATORS)["VAST_IMAGE"]
    image_tag = f"{type}-{int(time.time())}"
    version = conf(VALIDATORS)["VAST_VERSION"]
    if version == "build":
        c.run(f"docker build -t {base_image}:{image_tag} {REPOROOT}")
        version = image_tag
    c.run(
        f"""docker build \
            --build-arg VAST_VERSION={version} \
            --build-arg BASE_IMAGE={base_image} \
            -f {DOCKERDIR}/{type}.Dockerfile \
            -t {image_url}:{image_tag} \
            {RESOURCEDIR}"""
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
        tags=[{"Key": f"current-{type}", "Value": f"{image_url}:{image_tag}"}],
    )


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "core-2", "vast_task_family")
    service_name = terraform_output(c, "core-2", "vast_server_service_name")
    return (cluster, service_name, family)


@task
def vast_server_status(c):
    """Get the status of the VAST server"""
    print(FargateService(*service_outputs(c)).get_task_status())


@task
def start_vast_server(c):
    """Start the VAST server instance as an AWS Fargate task. Noop if a VAST server is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop_vast_server(c):
    """Stop the VAST server instance"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart_vast_server(c):
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


@pty_task
def destroy_step(c, step, auto_approve=False):
    """Destroy resources of the specified step. Resources depending on it should be cleaned up first."""
    init_step(c, step)
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
    )


@pty_task
def destroy(c, auto_approve=False):
    """Tear down the entire terraform stack with all the active plugins

    Note that if a module was deployed and the associated plugin was removed
    from the config afterwards, it will not be destroyed"""
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    serviceArns = aws("ecs").list_services(cluster=cluster, maxResults=100)[
        "serviceArns"
    ]
    for serviceArn in serviceArns:
        aws("ecs").update_service(cluster=cluster, service=serviceArn, desiredCount=0)
    c.run(
        f"terragrunt run-all destroy {auto_app_fmt(auto_approve)} {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
    )
