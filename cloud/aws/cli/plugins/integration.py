from vast_invoke import task, Context
import time
import json
from common import COMMON_VALIDATORS, AWS_REGION_VALIDATOR, container_path

VALIDATORS = [
    *COMMON_VALIDATORS,
    AWS_REGION_VALIDATOR,
]


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


@task
def workbucket_up_down(c):
    prefix = "testobject"
    key = f"{prefix}_key"
    src = f"/tmp/{prefix}"
    dst = f"/tmp/{prefix}"
    c.run(f"echo 'hello world' > {container_path(src)}")

    print("List before upload")
    c.run(f"./vast-cloud workbucket.delete --key={key}", hide="out")
    empty_ls = c.run(
        f"./vast-cloud workbucket.list --prefix={prefix}", hide="out"
    ).stdout
    assert empty_ls == "", f"Expected empty list, got {empty_ls}"

    print(f"Upload from {src} to object {key}")
    c.run(
        f"echo 'hello world' | ./vast-cloud workbucket.upload --source={src} --key={key}"
    )
    ls = c.run(f"./vast-cloud workbucket.list --prefix {prefix}", hide="out").stdout
    assert ls == f"{key}\n", f"Expected only {key}<newline> in list, got {ls}"

    print(f"Download object {key} to {dst}")
    c.run(
        f"./vast-cloud workbucket.download --destination={dst} --key={key}",
        hide="out",
    )
    c.run(f"diff {container_path(src)} {container_path(dst)}")

    print(f"Delete object {key}")
    c.run(f"rm -f {container_path(src)} {container_path(dst)}")
    c.run(f"./vast-cloud workbucket.delete --key={key}", hide="out")
    empty_ls = c.run(
        f"./vast-cloud workbucket.list --prefix {prefix}", hide="out"
    ).stdout
    assert empty_ls == "", f"Expected empty list, got {empty_ls}"


@task
def all(c):
    """Run the entire testbook.

    Notes:
    - VAST needs to be deployed beforehand with the workbucket plugin
    - This will affect the state of the current stack"""
    workbucket_up_down(c)
    vast_start_restart(c)
    vast_data_import(c)
