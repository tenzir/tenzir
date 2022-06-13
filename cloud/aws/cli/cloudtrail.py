from invoke import task
import dynaconf
import core


VALIDATORS = [
    dynaconf.Validator("VAST_AWS_REGION", must_exist=True),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_NAME", must_exist=True),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_REGION", must_exist=True),
]

TFDIR = "./terraform"


@task
def deploy(c, auto_approve=False):
    """Deploy the cloudtrail datasource"""
    c.run(
        f"terragrunt apply {core.auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/step-3",
        env=core.conf(VALIDATORS),
        pty=True,
    )
