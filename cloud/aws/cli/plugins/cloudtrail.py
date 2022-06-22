from invoke import task
import dynaconf
import core


VALIDATORS = [
    dynaconf.Validator("VAST_AWS_REGION", must_exist=True),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_NAME", must_exist=True),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_REGION", must_exist=True),
]

INVOKE_CONFIG = {}


@task
def deploy(c, auto_approve=False):
    """Deploy the cloudtrail datasource"""
    core.init_step(c, "cloudtrail")
    c.run(
        f"terragrunt apply {core.auto_app_fmt(auto_approve)} --terragrunt-working-dir {core.TFDIR}/cloudtrail",
        env=core.conf(VALIDATORS),
        pty=True,
    )


@task
def destroy(c, auto_approve=False):
    """Remove the cloudtrail datasource"""
    core.init_step(c, "cloudtrail")
    c.run(
        f"terragrunt destroy {core.auto_app_fmt(auto_approve)} --terragrunt-working-dir {core.TFDIR}/cloudtrail",
        env=core.conf(VALIDATORS),
        pty=True,
    )
