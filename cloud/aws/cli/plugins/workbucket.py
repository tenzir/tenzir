from vast_invoke import task
from common import (
    AWS_REGION,
    check_absolute,
    container_path,
    terraform_output,
    aws,
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


@task(help={"source": "Absolute path to the file, use /dev/stdin to pipe in data"})
def upload(c, source, key):
    """Upload a file to the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    if source == "/dev/stdin":
        # the `-` feature of `aws s3 cp` enables upload from non-seekable streams
        source = "-"
    else:
        check_absolute(source)
        source = container_path(source)
    c.run(
        f"aws s3 cp {source} s3://{name(c)}/{key} --region {AWS_REGION()}",
        hide="out",
    )


@task(
    help={"destination": "Absolute path to the file, use /dev/stdout to pipe out data"}
)
def download(c, key, destination):
    """Download a file to the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    if destination == "/dev/stdout":
        destination = "-"
    else:
        check_absolute(destination)
        destination = container_path(destination)
    c.run(
        f"aws s3 cp s3://{name(c)}/{key} {destination} --region {AWS_REGION()}",
        hide="out",
    )


@task
def delete(c, key):
    """Delete the specified key in the bucket

    This is a simplified helper, for more advanced features use the AWS CLI directly"""
    aws("s3").delete_object(Bucket=name(c), Key=key)
