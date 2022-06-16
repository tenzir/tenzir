from invoke import task, Context
import time
import dynaconf
import core
import json
import re
import tfcloud

VALIDATORS = [
    dynaconf.Validator("TF_ORGANIZATION", must_exist=True),
    dynaconf.Validator("TF_WORKSPACE_PREFIX", default="gh-act"),
    dynaconf.Validator("TF_API_TOKEN", must_exist=True),
    dynaconf.Validator("TF_STATE_BACKEND", default="local"),
]

TFDIR = "./terraform"


@task(autoprint=True)
def list_modules(c):
    deps = c.run(
        """terragrunt graph-dependencies""", hide="out", env=core.conf(VALIDATORS)
    ).stdout
    return re.findall('terraform/(.*)" ;', deps)


@task(autoprint=True)
def tf_version(c):
    version_json = c.run("terraform version -json", hide="out").stdout
    return json.loads(version_json)["terraform_version"]


@task
def config_tfcloud(c):
    conf = core.conf(VALIDATORS)
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
        [
            {"key": "AWS_SECRET_ACCESS_KEY", "sensitive": True},
            {"key": "AWS_ACCESS_KEY_ID", "sensitive": False},
        ],
    )
    for ws in ws_list:
        client.assign_varset(varset["id"], ws["id"])

    print("Set variables (Ctrl+c to cancel):")
    for var in client.get_variables(varset["id"]):
        val = input(f"{var['attributes']['key']}:")
        client.set_variable(varset["id"], var["id"], val)


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
    res = c.run('./vast-cloud run-lambda -c "vast count"', hide="stdout")
    return int(res.stdout.strip())


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
