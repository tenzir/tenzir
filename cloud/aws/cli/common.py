from functools import cache
from typing import Dict, List
from vast_invoke import Context, Exit
import dynaconf
import json
import re
import os
import subprocess
from boto3.session import Session
import boto3
import botocore.client


@cache
def s3_regions():
    """List all the regions where S3 is available using the AWS API"""
    return Session().get_available_regions("s3")


AWS_REGION_VALIDATOR = dynaconf.Validator(
    "VAST_AWS_REGION", must_exist=True, is_in=s3_regions()
)

# Common validators should always have defaults to avoid forcing a variable on a
# plugin that doesn't need it
# Side note: an empty variable is considered as existing
COMMON_VALIDATORS = [
    dynaconf.Validator("TF_STATE_BACKEND", default="local", is_in=["local", "cloud"]),
    dynaconf.Validator("TF_WORKSPACE_PREFIX", default=""),
    # if we use tf cloud as backend, the right variable must be configured
    dynaconf.Validator("TF_STATE_BACKEND", ne="cloud")
    | (
        dynaconf.Validator("TF_ORGANIZATION", must_exist=True, ne="")
        & dynaconf.Validator("TF_API_TOKEN", must_exist=True, ne="")
    ),
]

# Path aliases
CLOUDROOT = "."
REPOROOT = "../.."
TFDIR = f"{CLOUDROOT}/terraform"
DOCKERDIR = f"{CLOUDROOT}/docker"
HOSTROOT = "/host"


def conf(validators=[]) -> dict:
    """Load variables from both the environment and the .env file if:
    - their key is prefixed with either VAST_, TF_ or AWS_"""
    dc = dynaconf.Dynaconf(
        load_dotenv=True,
        envvar_prefix=False,
        validators=validators,
    )
    return {
        k: v
        for (k, v) in dc.as_dict().items()
        if k.startswith(("VAST_", "TF_", "AWS_"))
    }


def auto_app_fmt(val: bool) -> str:
    """Format the CLI options for auto approve"""
    if val:
        return "--terragrunt-non-interactive --auto-approve"
    else:
        return ""


def list_modules(c: Context):
    """List available Terragrunt modules"""
    deps = c.run(
        """terragrunt graph-dependencies""", hide="out", env=conf(COMMON_VALIDATORS)
    ).stdout
    return re.findall('terraform/(.*)" ;', deps)


def tf_version(c: Context):
    """Terraform version used by the CLI"""
    version_json = c.run("terraform version -json", hide="out").stdout
    return json.loads(version_json)["terraform_version"]


def terraform_output(c: Context, step, key) -> str:
    output = c.run(
        f"terraform -chdir={TFDIR}/{step} output --raw {key}",
        hide="out",
        # avoid unintentionally capturing stdin
        in_stream=False,
    ).stdout
    if "No outputs found" in output:
        raise Exit(
            f"The step '{step}' was not deployed or is improperly initialized (No outputs found)",
            code=1,
        )
    return output


def AWS_REGION():
    return conf(AWS_REGION_VALIDATOR)["VAST_AWS_REGION"]


def aws(service):
    # timeout set to 1000 to be larger than lambda max duration
    config = botocore.client.Config(retries={"max_attempts": 0}, read_timeout=1000)
    return boto3.client(service, region_name=AWS_REGION(), config=config)


def container_path(host_path: str):
    """Convert the given path on the host its location once mounted in the CLI container"""
    return f"{HOSTROOT}{host_path}"


def check_absolute(path: str):
    """Raise an Exit if the provided path is not absolute"""
    if not os.path.isabs(path):
        raise Exit(f"{path} is not an absolute path")


def load_cmd(cmd: str) -> bytes:
    """Load the command as bytes. If cmd starts with file:// or s3://, load commands from a file.

    Must be an absolute path (e.g file:///etc/mycommands)"""
    if cmd.startswith("file://"):
        path = cmd[7:]
        check_absolute(path)
        with open(container_path(path), "rb") as f:
            return f.read()
    if cmd.startswith("s3://"):
        chunks = cmd[5:].split("/", 1)
        return aws("s3").get_object(Bucket=chunks[0], Key=chunks[1])["Body"].read()
    else:
        return cmd.encode()


def default_vast_version():
    """If VAST_VERSION is not defined, use latest version"""
    # dynaconf cannot run default lazily
    if "VAST_VERSION" in os.environ:
        return
    cmd = "git describe --match='v[0-9]*' --abbrev=0 --exclude='*-rc*'"
    args = ["bash", "-c", cmd]
    return subprocess.check_output(args, text=True).rstrip()


def parse_env(env: List[str]) -> Dict[str, str]:
    """Convert a list of "key=value" strings to a dictionary of {key: value}"""

    def split_name_val(name_val):
        env_split = name_val.split("=")
        if len(env_split) != 2:
            raise Exit(f"{name_val} should have exactly one '=' char", 1)
        return env_split[0], env_split[1]

    name_val_list = [split_name_val(v) for v in env]
    return {v[0]: v[1] for v in name_val_list}
