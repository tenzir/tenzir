from invoke import Context
import dynaconf
import json
import re
from boto3.session import Session

AWS_REGIONS = Session().get_available_regions("s3")

# Note: an empty variable is considered as existing
COMMON_VALIDATORS = [
    dynaconf.Validator("VAST_AWS_REGION", must_exist=True, is_in=AWS_REGIONS),
    dynaconf.Validator("TF_STATE_BACKEND", default="local", is_in=["local", "cloud"]),
    dynaconf.Validator("TF_WORKSPACE_PREFIX", default=""),
    # if we use tf cloud as backend, the right variable must be configured
    dynaconf.Validator("TF_STATE_BACKEND", ne="cloud")
    | (
        dynaconf.Validator("TF_ORGANIZATION", must_exist=True, ne="")
        & dynaconf.Validator("TF_API_TOKEN", must_exist=True, ne="")
    ),
]

## Helper functions


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


# Aliases
AWS_REGION = conf(COMMON_VALIDATORS)["VAST_AWS_REGION"]
CLOUDROOT = "."
REPOROOT = "../.."
TFDIR = f"{CLOUDROOT}/terraform"
DOCKERDIR = f"{CLOUDROOT}/docker"
