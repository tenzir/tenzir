from vast import VAST, ExportMode, collect_pyarrow, to_rows, VastRow
import asyncio
import ipaddress
import os
import pytest
import subprocess
import time
import shutil

if "VAST_PYTHON_INTEGRATION" not in os.environ:
    # Tests in this module require access to integration test files and the VAST binary
    pytest.skip(
        "VAST_PYTHON_INTEGRATION not defined, skipping vast tests",
        allow_module_level=True,
    )

TEST_DB_DIR = "/tmp/test-vast-db"


@pytest.fixture(autouse=True)
def vast_server():
    proc = subprocess.Popen(["vast", "-d", TEST_DB_DIR, "start"])
    time.sleep(3)
    yield
    proc.kill()
    shutil.rmtree(TEST_DB_DIR)


def vast_import(expression: list[str], file: str):
    proc = subprocess.Popen(
        ["vast", "import", "--blocking", *expression], stdin=subprocess.PIPE
    )
    with open(file, "rb") as f:
        suricata_bytes = f.read()
        proc.stdin.write(suricata_bytes)
        proc.stdin.close()
    assert proc.wait() == 0
    subprocess.run(["vast", "flush"])


def integration_data(path):
    return f"../../vast/integration/data/{path}"


@pytest.mark.asyncio
async def test_count():
    result = await VAST.count()
    assert result == 0
    vast_import(["suricata"], integration_data("suricata/eve.json"))
    result = await VAST.count()
    assert result == 7


@pytest.mark.asyncio
async def test_export_collect_pyarrow():
    vast_import(["suricata"], integration_data("suricata/eve.json"))
    result = VAST.export('#type == "suricata.alert"', ExportMode.HISTORICAL)
    tables = await collect_pyarrow(result)
    assert set(tables.keys()) == {"suricata.alert"}
    alerts = tables["suricata.alert"]
    assert len(alerts) == 1
    assert alerts[0].num_rows == 1

    result = VAST.export("", ExportMode.HISTORICAL)
    tables = await collect_pyarrow(result)
    assert set(tables.keys()) == {
        "suricata.alert",
        "suricata.dns",
        "suricata.netflow",
        "suricata.flow",
        "suricata.fileinfo",
        "suricata.http",
        "suricata.stats",
    }


@pytest.mark.asyncio
async def test_export_historical_row():
    vast_import(["suricata"], integration_data("suricata/eve.json"))
    result = VAST.export('#type == "suricata.alert"', ExportMode.HISTORICAL)
    rows: list[VastRow] = []
    async for row in to_rows(result):
        rows.append(row)
    assert len(rows) == 1
    assert rows[0].name == "suricata.alert"
    # only assert extension types here
    alert = rows[0].data
    assert alert["src_ip"] == ipaddress.IPv4Address("147.32.84.165")
    assert alert["dest_ip"] == ipaddress.IPv4Address("78.40.125.4")


@pytest.mark.asyncio
async def test_export_continuous_rows():
    async def run_export():
        result = VAST.export('#type == "suricata.alert"', ExportMode.CONTINUOUS)
        return await anext(result)

    task = asyncio.create_task(run_export())
    await asyncio.sleep(3)
    await asyncio.get_event_loop().run_in_executor(
        None, lambda: vast_import(["suricata"], integration_data("suricata/eve.json"))
    )
    print("await anext", flush=True)
    row = await task
    print("alert received", flush=True)
    assert row.name == "suricata.alert"
    # only assert extension types here
    alert = row.data
    assert alert["src_ip"] == ipaddress.IPv4Address("147.32.84.165")
    assert alert["dest_ip"] == ipaddress.IPv4Address("78.40.125.4")
