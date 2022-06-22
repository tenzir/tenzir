import dynaconf

COMMON_VALIDATORS = [
    dynaconf.Validator("VAST_AWS_REGION", must_exist=True),
    dynaconf.Validator("TF_STATE_BACKEND", default="local"),
    dynaconf.Validator("TF_WORKSPACE_PREFIX", default="gh-act"),
    # if we use tf cloud as backend, the right variable must be configured
    dynaconf.Validator("TF_STATE_BACKEND", ne="cloud")
    | (
        dynaconf.Validator("TF_ORGANIZATION", must_exist=True)
        & dynaconf.Validator("TF_API_TOKEN", must_exist=True)
    ),
]


def conf(validators=[]) -> dict:
    """Load config starting with VAST_, TF_ or AWS_ from both env variables and
    .env file"""
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


# Aliases
AWS_REGION = conf(COMMON_VALIDATORS)["VAST_AWS_REGION"]
CLOUDROOT = "."
REPOROOT = "../.."
TFDIR = f"{CLOUDROOT}/terraform"
DOCKERDIR = f"{CLOUDROOT}/docker"


def auto_app_fmt(val: bool) -> str:
    if val:
        return "--terragrunt-non-interactive --auto-approve"
    else:
        return ""
