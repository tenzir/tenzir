from vast import VAST, ExportMode, collect_pyarrow, to_rows, VastRow
import vast.utils.logging
import asyncio
from asyncio.subprocess import PIPE
import ipaddress
import os
import pytest
import shutil

logger = vast.utils.logging.get("vast.test")

if "VAST_PYTHON_INTEGRATION" not in os.environ:
    # Tests in this module require access to integration test files and the VAST binary
    pytest.skip(
        "VAST_PYTHON_INTEGRATION not defined, skipping vast tests",
        allow_module_level=True,
    )

TEST_DB_DIR = "/tmp/test-vast-db"


@pytest.fixture(autouse=True)
async def vast_server():
    proc = await asyncio.create_subprocess_exec(
        "vast", "-d", TEST_DB_DIR, "start", stderr=asyncio.subprocess.PIPE
    )
    await asyncio.sleep(3)
    yield
    proc.kill()
    await asyncio.to_thread(shutil.rmtree, TEST_DB_DIR)


async def vast_import(expression: list[str]):
    # import
    logger.debug(f"> vast import --blocking {' '.join(expression)}")
    import_proc = await asyncio.create_subprocess_exec(
        "vast", "import", "--blocking", *expression, stderr=PIPE
    )
    (_, import_err) = await asyncio.wait_for(import_proc.communicate(), 3)
    assert import_proc.returncode == 0
    logger.debug(f"vast import stderr:\n{import_err.decode()}")
    # flush
    logger.debug(f"> vast flush")
    flush_proc = await asyncio.create_subprocess_exec("vast", "flush", stderr=PIPE)
    (_, flush_err) = await asyncio.wait_for(flush_proc.communicate(), 3)
    assert flush_proc.returncode == 0
    logger.debug(f"vast flush stderr:\n{flush_err.decode()}")


def integration_data(path):
    return f"../../vast/integration/data/{path}"


@pytest.mark.asyncio
async def test_count():
    result = await VAST.count()
    assert result == 0
    await vast_import(["-r", integration_data("suricata/eve.json"), "suricata"])
    result = await VAST.count()
    assert result == 7


@pytest.mark.asyncio
async def test_export_collect_pyarrow():
    await vast_import(["-r", integration_data("suricata/eve.json"), "suricata"])
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
    await vast_import(["-r", integration_data("suricata/eve.json"), "suricata"])
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
        logger.info("export returned")
        async for row in to_rows(result):
            logger.info("return row")
            return row

    task = asyncio.create_task(run_export())
    await asyncio.sleep(6)
    await vast_import(["-r", integration_data("suricata/eve.json"), "suricata"])
    logger.info("await task")
    row = await task
    logger.info("task awaited")
    assert row is not None
    assert row.name == "suricata.alert"
    # # only assert extension types here
    # alert = row.data
    # assert alert["src_ip"] == ipaddress.IPv4Address("147.32.84.165")
    # assert alert["dest_ip"] == ipaddress.IPv4Address("78.40.125.4")
