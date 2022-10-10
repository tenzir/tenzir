from typing import Tuple
from vast_invoke import Context, pty_task, task, Exit
import dynaconf
import time
import base64
import json
import io
from common import (
    active_modules,
    clean_modules,
    conf,
    TFDIR,
    auto_app_fmt,
    REPOROOT,
    DOCKERDIR,
    AWS_REGION_VALIDATOR,
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
    dynaconf.Validator("VAST_VERSION", default="latest"),
    dynaconf.Validator("VAST_STORAGE_TYPE", default="EFS", is_in=["EFS", "ATTACHED"]),
]


def active_include_dirs(c: Context) -> str:
    """The --include-dir arguments for modules activated and core modules"""
    return " ".join([f"--terragrunt-include-dir={mod}" for mod in active_modules(c)])


def docker_compose(step):
    """The docker compose command in the directory of the specified step"""
    return f"docker compose --project-directory {DOCKERDIR}/{step}"


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


def init_step(c, step):
    """Manually run terraform init on a specific step"""
    mods = active_modules(c)
    if step not in mods:
        raise Exit(f"Step {step} not part of the active modules {mods}")
    c.run(
        f"terragrunt init --terragrunt-working-dir {TFDIR}/{step}",
    )


@task(help={"clean": clean_modules.__doc__})
def init(c, step="", clean=False):
    """Manually run terraform init on a specific step or all of them"""
    if clean:
        clean_modules()
    if step == "":
        c.run(
            f"terragrunt run-all init {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
        )
    else:
        init_step(c, step)


def deploy_step(c, step, auto_approve=False):
    """Deploy only one step of the stack"""
    init_step(c, step)
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
    )


@pty_task
def deploy(c, step="", auto_approve=False):
    """One liner build and deploy of the stack to AWS with all the modules associated with active plugins"""
    if step == "":
        c.run(
            f"terragrunt run-all apply {auto_app_fmt(auto_approve)} {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
        )
    else:
        deploy_step(c, step, auto_approve)


@task(
    autoprint=True,
    help={
        "service": "The qualifier of the service this image will be used for, as specified in deploy-image"
    },
)
def current_image(c, service):
    """Get the current Lambda image URI. In case of failure, returns the error message instead of the URI."""
    repo_arn = terraform_output(c, "core-1", f"vast_repository_arn")
    try:
        tags = aws("ecr").list_tags_for_resource(resourceArn=repo_arn)["tags"]
    except Exception as e:
        return str(e)
    current = next(
        (tag["Value"] for tag in tags if tag["Key"] == f"current-{service}"),
        "current-image-not-defined",
    )
    return current


@task
def build_images(c, step):
    """Build the provided VAST based Dockerfile using the configured base image
    and version"""
    if conf(VALIDATORS)["VAST_VERSION"] == "build":
        c.run(f"docker build -t $VAST_IMAGE:build {REPOROOT}")
    c.run(f"{docker_compose(step)} build")


def deploy_image(c, service, tag):
    """Push the provided image to the core image repository"""
    ## We are using the repository tags as a key value store to flag
    ## the current image of each service. This allows a controlled
    ## version rollout in the downstream infra (lambda or fargate)
    image_url = terraform_output(c, "core-1", f"vast_repository_url")
    repo_arn = terraform_output(c, "core-1", f"vast_repository_arn")
    # get the digest of the current image
    try:
        current_img = current_image(c, service)
        c.run(f"docker pull {current_img}")
        old_digest = c.run(
            f"docker inspect --format='{{{{.RepoDigests}}}}' {current_img}",
            hide="out",
        ).stdout
    except:
        old_digest = "current-image-not-found"
    # get the new digest
    new_digest = c.run(
        f"docker inspect --format='{{{{.RepoDigests}}}}' {tag}",
        hide="out",
    ).stdout
    # compare old an new digests
    if old_digest == new_digest:
        print("Docker image didn't change, skipping push")
        return
    # if a change occured, push and tag the new image as current
    ecr_tag = f"{image_url}:{service}-{int(time.time())}"
    c.run(f"docker image tag {tag} {ecr_tag}")
    c.run(f"docker push {ecr_tag}")
    c.run(f"docker rmi {ecr_tag}")
    aws("ecr").tag_resource(
        resourceArn=repo_arn,
        tags=[{"Key": f"current-{service}", "Value": f"{ecr_tag}"}],
    )


@task
def push_images(c, step):
    """Push the images specified in the docker compose for that step"""
    cf_str = c.run(f"{docker_compose(step)} convert --format json", hide="out").stdout
    cf_dict = json.loads(cf_str)["services"]
    for svc in cf_dict.items():
        deploy_image(c, svc[0], svc[1]["image"])


@task
def print_image_vars(c, step):
    """Display the tfvars file with the image tags.

    The output variable name for each service is the service name (as defined in
    the docker compose file) suffixed by "_image" """
    cf_str = c.run(f"{docker_compose(step)} convert --format json", hide="out").stdout
    cf_dict = json.loads(cf_str)["services"]
    for svc_name in cf_dict.keys():
        print(f'{svc_name}_image = "{current_image(c, svc_name)}"')


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "core-2", "vast_task_family")
    service_name = terraform_output(c, "core-2", "vast_server_service_name")
    return (cluster, service_name, family)


def destroy_step(c, step, auto_approve=False):
    """Destroy resources of the specified step. Resources depending on it should be cleaned up first."""
    init_step(c, step)
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/{step}",
    )


def stop_all_services(c):
    try:
        cluster = terraform_output(c, "core-2", "fargate_cluster_name")
        serviceArns = aws("ecs").list_services(cluster=cluster, maxResults=100)[
            "serviceArns"
        ]
        for serviceArn in serviceArns:
            aws("ecs").update_service(
                cluster=cluster, service=serviceArn, desiredCount=0
            )
    except Exception as e:
        print(e)
        print("Could not stop services, continuing with destroy...")


@pty_task
def destroy(c, step="", auto_approve=False):
    """Tear down the entire terraform stack with all the active plugins

    Note that if a module was deployed and the associated plugin was removed
    from the config afterwards, it will not be destroyed"""
    if step == "":
        stop_all_services(c)
        c.run(
            f"terragrunt run-all destroy {auto_app_fmt(auto_approve)} {active_include_dirs(c)} --terragrunt-working-dir {TFDIR}",
        )
    else:
        destroy_step(c, step, auto_approve)
