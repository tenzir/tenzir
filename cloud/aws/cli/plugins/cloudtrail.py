from vast_invoke import pty_task
import dynaconf
import core
from common import COMMON_VALIDATORS, auto_app_fmt, conf, TFDIR, AWS_REGION_VALIDATOR


VALIDATORS = [
    *COMMON_VALIDATORS,
    AWS_REGION_VALIDATOR,
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_REGION", must_exist=True, ne=""),
]


@pty_task
def deploy(c, auto_approve=False):
    """Deploy the cloudtrail datasource"""
    core.init_step(c, "cloudtrail")
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/cloudtrail",
        env=conf(VALIDATORS),
    )


@pty_task
def destroy(c, auto_approve=False):
    """Remove the cloudtrail datasource"""
    core.init_step(c, "cloudtrail")
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/cloudtrail",
        env=conf(VALIDATORS),
    )
