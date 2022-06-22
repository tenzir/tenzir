from invoke import task, Context
import time
import json
import re
import tfcloud
from common import COMMON_VALIDATORS, conf

VALIDATORS = COMMON_VALIDATORS

INVOKE_CONFIG = {"run": {"env": {"VASTCLOUD_NOTTY": "1"}}}


@task(autoprint=True)
def list_modules(c):
    """List available Terragrunt modules"""
    deps = c.run(
        """terragrunt graph-dependencies""", hide="out", env=conf(VALIDATORS)
    ).stdout
    return re.findall('terraform/(.*)" ;', deps)


@task(autoprint=True)
def tf_version(c):
    """Terraform version used by the CLI"""
    version_json = c.run("terraform version -json", hide="out").stdout
    return json.loads(version_json)["terraform_version"]


@task(
    help={
        "auto": """if set to True, this will forward the values of your 
current environement variables. Otherwise you will be prompted for 
the values you want to give to the environment variables"""
    }
)
def config_tfcloud(c, auto=False):
    """Configure workspaces in your Terrraform Cloud account."""
    conf = conf(VALIDATORS)
    client = tfcloud.Client(
        conf["TF_ORGANIZATION"],
        conf["TF_API_TOKEN"],
    )
    ws_list = client.upsert_workspaces(
        conf["TF_WORKSPACE_PREFIX"],
        list_modules(c),
        tf_version(c),
        "cloud/aws/terraform",
    )

    varset = client.create_varset(
        f"{conf['TF_WORKSPACE_PREFIX']}-aws-creds",
    )
    for ws in ws_list:
        client.assign_varset(varset["id"], ws["id"])

    var_defs = [
        {"key": "AWS_SECRET_ACCESS_KEY", "sensitive": True},
        {"key": "AWS_ACCESS_KEY_ID", "sensitive": False},
    ]
    for var_def in var_defs:
        if auto:
            value = conf[var_def["key"]]
        else:
            value = input(f"{var_def['key']} (Ctrl+c to cancel):")
        client.set_variable(varset["id"], var_def["key"], value, var_def["sensitive"])


@task
def vast_start_restart(c):
    """Validate VAST server start and restart commands"""
    print("Run start-vast-server")
    c.run("./vast-cloud start-vast-server")

    print("Get running vast server")
    c.run("./vast-cloud get-vast-server")

    print("Run start-vast-server again")
    c.run("./vast-cloud start-vast-server")

    print("Run restart-vast-server")
    c.run("./vast-cloud restart-vast-server")

    print("Get running vast server")
    c.run("./vast-cloud get-vast-server")

    print("The task needs a bit of time to boot, sleeping for a while...")
    time.sleep(100)


def vast_import_suricata(c: Context):
    """Import Suricata data from the provided URL"""
    url = "https://raw.githubusercontent.com/tenzir/vast/v1.0.0/vast/integration/data/suricata/eve.json"
    c.run(
        f"./vast-cloud execute-command -c 'wget -O - -o /dev/null {url} | vast import suricata'"
    )


def vast_count(c: Context):
    """Run `vast count` and parse the result"""
    res_raw = c.run('./vast-cloud run-lambda -c "vast count"', hide="stdout")
    res_obj = json.loads(res_raw.stdout)
    assert res_obj["parsed_cmd"] == [
        "/bin/bash",
        "-c",
        "vast count",
    ], "Unexpected parsed command"
    assert res_obj["stdout"].isdigit(), "Count result is not a number"
    return int(res_obj["stdout"])


@task
def vast_data_import(c):
    """Import data from a file and check count increase"""
    print("Import data into VAST")
    init_count = vast_count(c)
    vast_import_suricata(c)
    new_count = vast_count(c)
    print(f"Expected count increase 7, got {new_count-init_count}")
    assert 7 == new_count - init_count, "Wrong count"


@task()
def all(c):
    """Run the entire testbook. VAST needs to be deployed beforehand.
    Warning: This will affect the state of the current stack"""
    vast_start_restart(c)
    vast_data_import(c)
