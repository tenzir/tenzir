from typing import Tuple
from vast_invoke import Context, task
import dynaconf
from common import (
    RESOURCEDIR,
    FargateService,
    DOCKERDIR,
    terraform_output,
)

VALIDATORS = [dynaconf.Validator("VAST_MISP_VERSION", default="v2.4.145a")]


@task(help={"tag": "The tag of the built image"})
def build_image(c, tag):
    """Build a MISP image with the appropriate startup script"""
    c.run(
        f"""docker build \
            --build-arg MISP_VERSION=$VAST_MISP_VERSION \
            -f {DOCKERDIR}/misp.Dockerfile \
            -t {tag} \
            {RESOURCEDIR}"""
    )


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "misp", "misp_task_family")
    service_name = terraform_output(c, "misp", "misp_service_name")
    return (cluster, service_name, family)


@task
def status(c):
    """Get the status of the MISP service"""
    print(FargateService(*service_outputs(c)).get_task_status())


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
