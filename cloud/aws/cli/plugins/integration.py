from invoke import task, Context
import time
import json
from common import COMMON_VALIDATORS, AWS_REGION_VALIDATOR

VALIDATORS = [
    *COMMON_VALIDATORS,
    AWS_REGION_VALIDATOR,
]

INVOKE_CONFIG = {"run": {"env": {"VASTCLOUD_NOTTY": "1"}}}


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
