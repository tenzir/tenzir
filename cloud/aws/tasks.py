from invoke import task, Context, Exit
import boto3
import botocore.client
import os
from dotenv import load_dotenv
import time
import base64
import json

load_dotenv()
AWS_REGION = os.getenv("aws_region")
EXIT_CODE_VAST_SERVER_NOT_RUNNING = 8


def aws(service):
    # timeout set to 1000 to be larger than lambda max duration
    config = botocore.client.Config(retries={"max_attempts": 0}, read_timeout=1000)
    return boto3.client(service, region_name=AWS_REGION, config=config)


def _terraform_output(c: Context, step, key) -> str:
    return c.run(f"terraform -chdir={step} output --raw {key}", hide="out").stdout


def _vast_lambda_image(c: Context) -> str:
    if os.getenv("vast_lambda_image") is not None:
        return os.getenv("vast_lambda_image")
    repo_arn = _terraform_output(c, "step-1", "vast_lambda_repository_arn")
    tags = aws("ecr").list_tags_for_resource(resourceArn=repo_arn)["tags"]
    current = next((tag["Value"] for tag in tags if tag["Key"] == "current"))
    return current


def _vast_version(c: Context):
    # TODO use git describe
    return os.getenv("vast_version", "v1.1.0")


def _step_1_variables() -> dict:
    return {
        "TF_VAR_region_name": AWS_REGION,
    }


def _step_2_variables(c: Context) -> dict:
    return {
        "TF_VAR_vast_version": _vast_version(c),
        "TF_VAR_vast_server_storage_type": os.getenv("vast_server_storage_type", "EFS"),
        "TF_VAR_peered_vpc_id": os.getenv("peered_vpc_id"),
        "TF_VAR_vast_cidr": os.getenv("vast_cidr"),
        "TF_VAR_region_name": AWS_REGION,
        "TF_VAR_vast_lambda_image": _vast_lambda_image(c),
    }


def _auto_approve(val: bool) -> str:
    if val:
        return "-auto-approve"
    else:
        return ""


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
        f"docker login --username {user_pass[0]} --password {user_pass[1]} {registry}"
    )


@task
def deploy_step_1(c, auto_approve=False):
    """Deploy only step 1 of the stack"""
    env = _step_1_variables()
    c.run('terraform -chdir="step-1" init', env=env)
    c.run(f'terraform -chdir="step-1" apply {_auto_approve(auto_approve)}', env=env)


@task
def lambda_image(c):
    """Build and push the lambda image, fails if step 1 is not deployed"""
    image_url = _terraform_output(c, "step-1", "vast_lambda_repository_url")
    image_tag = int(time.time())
    c.run(
        f"docker build --build-arg VAST_VERSION={_vast_version(c)} -f docker/lambda.Dockerfile -t {image_url}:{image_tag} ."
    )
    c.run(f"docker push {image_url}:{image_tag}")
    image_arn = _terraform_output(c, "step-1", "vast_lambda_repository_arn")
    aws("ecr").tag_resource(
        resourceArn=image_arn,
        tags=[{"Key": "current", "Value": f"{image_url}:{image_tag}"}],
    )


@task
def deploy_step_2(c, auto_approve=False):
    """Deploy only step 2 of the stack"""
    env = _step_2_variables(c)
    c.run('terraform -chdir="step-2" init', env=env)
    c.run(f'terraform -chdir="step-2" apply {_auto_approve(auto_approve)}', env=env)


@task
def deploy(c, auto_approve=False):
    """One liner build and deploy of the stack to AWS"""
    deploy_step_1(c, auto_approve)
    docker_login(c)
    lambda_image(c)
    deploy_step_2(c, auto_approve)


@task
def destroy(c):
    """Tear down the entire terraform stack"""
    env = _step_2_variables(c)
    c.run('terraform -chdir="step-2" destroy', env=env)
    env = _step_1_variables()
    c.run('terraform -chdir="step-1" destroy', env=env)


@task
def run_vast_task(c):
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    subnet = _terraform_output(c, "step-2", "ids_appliances_subnet_id")
    sg = _terraform_output(c, "step-2", "vast_security_group")
    task_def = _terraform_output(c, "step-2", "vast_task_definition")
    task_res = aws("ecs").run_task(
        cluster=cluster,
        count=1,
        enableECSManagedTags=True,
        enableExecuteCommand=True,
        propagateTags="TASK_DEFINITION",
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": [subnet],
                "securityGroups": [sg],
                "assignPublicIp": "DISABLED",
            }
        },
        taskDefinition=task_def,
    )
    task_arn = task_res["tasks"][0]["taskArn"].split("/")[-1]
    print(f"Started task {task_arn}")


@task(autoprint=True)
def get_vast_server(c):
    """Get the task id of the VAST server"""
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    family = _terraform_output(c, "step-2", "vast_task_family")
    task_res = aws("ecs").list_tasks(family=family, cluster=cluster)
    nb_vast_tasks = len(task_res["taskArns"])
    if nb_vast_tasks == 0:
        raise Exit("No VAST server running", EXIT_CODE_VAST_SERVER_NOT_RUNNING)
    if nb_vast_tasks > 1:
        raise Exit(f"{nb_vast_tasks} VAST server running", 1)

    task_id = task_res["taskArns"][0].split("/")[-1]
    return task_id


@task(autoprint=True)
def describe_vast_server(c):
    """Get a complete description of the VAST server"""
    task_id = get_vast_server(c)
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    task_res = aws("ecs").describe_tasks(cluster=cluster, tasks=[task_id])
    meta = {
        "task_id": task_id,
        "ip": task_res["tasks"][0]["containers"][0]["networkInterfaces"][0][
            "privateIpv4Address"
        ],
        "runtime_id": task_res["tasks"][0]["containers"][0]["runtimeId"],
    }
    return meta


@task
def start_vast_server(c):
    try:
        get_vast_server(c)
    except Exit as e:
        if e.code == EXIT_CODE_VAST_SERVER_NOT_RUNNING:
            run_vast_task(c)


@task
def restart_vast_server(c):
    try:
        task_id = get_vast_server(c)
        cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
        aws("ecs").stop_task(task=task_id, cluster=cluster)
        print(f"Stopped task {task_id}")
    except Exit as e:
        if e.code != EXIT_CODE_VAST_SERVER_NOT_RUNNING:
            raise e
    start_vast_server(c)


@task(autoprint=True)
def list_all_tasks(c):
    """List the ids of all tasks running on the ECS cluster"""
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    task_res = aws("ecs").list_tasks(cluster=cluster)
    task_ids = [task.split("/")[-1] for task in task_res["taskArns"]]
    return task_ids


@task
def stop_all_tasks(c):
    """Stop all running tasks on the ECS cluster created by Terraform"""
    task_ids = list_all_tasks(c)
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    for task_id in task_ids:
        aws("ecs").stop_task(task=task_id, cluster=cluster)
        print(f"Stopped task {task_id}")


@task(autoprint=True)
def run_lambda(c, cmd):
    lambda_name = _terraform_output(c, "step-2", "vast_lambda_name")
    task_ip = describe_vast_server(c)["ip"]
    cmd_b64 = base64.b64encode(cmd.encode()).decode()
    lambda_res = aws("lambda").invoke(
        FunctionName=lambda_name,
        Payload=json.dumps({"cmd": cmd_b64, "host": f"{task_ip}:42000"}).encode(),
        InvocationType="RequestResponse",
    )
    if "FunctionError" in lambda_res:
        mess = f'{lambda_res["FunctionError"]}: {lambda_res["Payload"].read()}'
        raise Exit(message=mess, code=1)
    res = json.loads(lambda_res["Payload"].read())["result"]
    return res


@task
def execute_command(c, cmd="/bin/bash"):
    """Run ad-hoc or interactive commands from the VAST server Fargate task"""
    task_id = get_vast_server(c)
    cluster = _terraform_output(c, "step-2", "fargate_cluster_name")
    # if we are not running the default interactive shell, encode the command to avoid escaping issues
    if cmd != "/bin/bash":
        cmd = "/bin/bash -c 'echo {base64.b64encode(cmd.encode()).decode()} | base64 -d | /bin/bash"
    # we use the CLI here because boto does not know how to use the session-manager-plugin
    c.run(
        f"""aws ecs execute-command \
		--cluster {cluster} \
		--task {task_id} \
		--interactive \
		--command "{cmd}" \
        --region {AWS_REGION} """
    )
