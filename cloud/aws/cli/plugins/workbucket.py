from vast_invoke import task
import core
from common import COMMON_VALIDATORS, auto_app_fmt, conf, TFDIR, terraform_output, aws


VALIDATORS = COMMON_VALIDATORS


@task
def deploy(c, auto_approve=False):
    """Deploy a bucket that can be used to persist data from the client"""
    core.init_step(c, "workbucket")
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/workbucket",
        env=conf(VALIDATORS),
        pty=True,
    )


@task
def destroy(c, auto_approve=False):
    """Destroy the bucket"""
    core.init_step(c, "workbucket")
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/workbucket",
        env=conf(VALIDATORS),
        pty=True,
    )


@task(autoprint=True)
def name(c):
    """Print the name of the bucket"""
    return terraform_output(c, "workbucket", "bucket_name")


@task
def list(c, prefix=""):
    """List the content of the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    resp = aws("s3").list_objects_v2(Bucket=name(c), Delimiter="/", Prefix=prefix)
    if "Contents" in resp:
        for obj in resp["Contents"]:
            print(obj["Key"])


@task
def upload(c, source, key):
    """Upload a file to the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    aws("s3").upload_file(source, name(c), key)


@task
def download(c, key, destination):
    """Download a file to the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    aws("s3").download_file(name(c), key, destination)


@task
def delete(c, key):
    """Delete the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    aws("s3").delete_object(Bucket=name(c), Key=key)
