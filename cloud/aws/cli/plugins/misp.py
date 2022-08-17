from common import (
    DOCKERDIR,
    RESOURCEDIR,
    TFDIR,
    auto_app_fmt,
    conf,
    deploy_step_helper,
    destroy_step_helper,
    init_step_helper,
)
import dynaconf
from vast_invoke import pty_task, task
import core

VALIDATORS = [
    *core.VALIDATORS,
    dynaconf.Validator("VAST_MISP_VERSION", default="v2.4.145a"),
]


@pty_task
def deploy(c, auto_approve=False):
    """Deploy the misp module"""
    init_step_helper(c, "misp", VALIDATORS)
    deploy_step_helper(c, "misp", VALIDATORS, auto_approve)


@pty_task
def destroy(c, auto_approve=False):
    """Remove the misp module"""
    init_step_helper(c, "misp", VALIDATORS)
    destroy_step_helper(c, "misp", VALIDATORS, auto_approve)


@task(help={"tag": "The tag of the built image"})
def build_misp_image(c, tag):
    """Build the provided VAST based Dockerfile using the configured base image
    and version"""
    misp_version = conf(VALIDATORS)["VAST_MISP_VERSION"]
    c.run(
        f"""docker build \
            --build-arg MISP_VERSION={misp_version} \
            -f {DOCKERDIR}/misp.Dockerfile \
            -t {tag} \
            {RESOURCEDIR}"""
    )
