from vast_invoke import pty_task
import dynaconf
import core
from common import auto_app_fmt, conf, TFDIR


VALIDATORS = [
    *core.VALIDATORS,
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_REGION", must_exist=True, ne=""),
]


@pty_task
def deploy(c, auto_approve=False):
    """Deploy the VPC FLow Logs datasource"""
    core.init_step(c, "flowlogs")
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/flowlogs",
        env=conf(VALIDATORS),
    )


@pty_task
def destroy(c, auto_approve=False):
    """Remove the VPC FLow Logs datasource"""
    core.init_step(c, "flowlogs")
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/flowlogs",
        env=conf(VALIDATORS),
    )
