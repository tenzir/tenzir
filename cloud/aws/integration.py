from invoke import task
import time


@task
def vast_start_restart(c):
    """Validate VAST server start and restart commands"""
    print("Run start-vast-server")
    c.run("./vast-cloud start-vast-server")

    print("Run start-vast-server again")
    res = c.run("./vast-cloud start-vast-server", warn=True)
    assert res.exited != 0, "Starting server again should fail"

    print("Run restart-vast-server")
    c.run("./vast-cloud restart-vast-server")

    print("Get running vast server")
    c.run("./vast-cloud get-vast-server")

    print("The task needs a bit of time to boot, sleeping for a while...")
    time.sleep(100)


@task
def vast_import_suricata(c):
    """Import Suricata data from the provided URL"""
    url = "https://raw.githubusercontent.com/tenzir/vast/v1.0.0/vast/integration/data/suricata/eve.json"
    c.run(
        f"./vast-cloud execute-command -c 'wget -O - -o /dev/null {url} | vast import suricata'"
    )


@task
def vast_count(c, count):
    """Validate that the event count in the running server"""
    res = c.run('./vast-cloud run-lambda -c "vast count"', hide="stdout")
    vast_count = res.stdout.strip()
    print(f"Expected vast count {count}, got {vast_count}")
    assert str(count) == vast_count, "Wrong count"


@task
def clean_context(c):
    """Remove existing resources that might pertubate the tests"""
    print("Stop all existing tasks")
    c.run("./vast-cloud stop-all-tasks")
    print("Clean mounted folder")
    c.run('./vast-cloud run-vast-task --cmd-override "rm -rf /var/lib/vast/*"')
    print("Waiting for the cleaning task to boot...")
    time.sleep(100)


@task(help={"clean-ctx": "Clean up resources before and after running the tests"})
def all(c, clean_ctx=False):
    """Run the entire testbook. VAST needs to be deployed beforehand.
    Warning: This will affect the state of the current stack"""
    if clean_ctx:
        clean_context(c)
    vast_start_restart(c)
    vast_count(c, 0)
    vast_import_suricata(c)
    vast_count(c, 7)
    if clean_ctx:
        clean_context(c)
