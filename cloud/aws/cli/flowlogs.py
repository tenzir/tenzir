from invoke import task
import dynaconf
import core


VALIDATORS = [
    dynaconf.Validator("VAST_AWS_REGION", must_exist=True),
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_NAME", must_exist=True),
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_REGION", must_exist=True),
]

TFDIR = "./terraform"


@task
def deploy(c, auto_approve=False):
    """Deploy the VPC FLow Logs datasource"""
    core.init_step(c, "flowlogs")
    c.run(
        f"terragrunt apply {core.auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/flowlogs",
        env=core.conf(VALIDATORS),
        pty=True,
    )


@task
def destroy(c, auto_approve=False):
    """Remove the VPC FLow Logs datasource"""
    core.init_step(c, "flowlogs")
    c.run(
        f"terragrunt destroy {core.auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/flowlogs",
        env=core.conf(VALIDATORS),
        pty=True,
    )
