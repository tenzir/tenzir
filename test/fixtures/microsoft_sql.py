"""Microsoft SQL Server fixture for database integration testing.

Provides a SQL Server Developer Edition instance for testing the
from_microsoft_sql operator.

Environment variables yielded:
- MSSQL_HOST: Server hostname (127.0.0.1)
- MSSQL_PORT: Server port (dynamically allocated)
- MSSQL_USER: Test username
- MSSQL_PASSWORD: Test password
- MSSQL_DATABASE: Pre-created test database
"""

from __future__ import annotations

import logging
import shlex
import threading
import uuid
from dataclasses import dataclass
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
from tenzir_test.fixtures.container_runtime import (
    ContainerCommandError,
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
MSSQL_SA_PASSWORD = "TenzirTest123!"
MSSQL_USER = "tenzir"
MSSQL_PASSWORD = "N7#vR2!qLm9@"
MSSQL_DATABASE = "tenzir_test"
STARTUP_TIMEOUT = 180
HEALTH_CHECK_INTERVAL = 2
LIVE_STREAM_TABLE = "dbo.users_live_stream"
LIVE_STREAM_TOKEN = "live-stream-token"
LIVE_STREAM_INITIAL_DELAY = 5.0
LIVE_STREAM_INSERT_INTERVAL = 0.1


@dataclass(frozen=True)
class MicrosoftSqlOptions:
    streaming: bool = False


def _start_microsoft_sql(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    container_name = f"tenzir-test-microsoft-sql-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:1433",
        "-e",
        "ACCEPT_EULA=Y",
        "-e",
        "MSSQL_PID=Developer",
        "-e",
        f"MSSQL_SA_PASSWORD={MSSQL_SA_PASSWORD}",
        MSSQL_IMAGE,
    ]
    logger.info("Starting Microsoft SQL Server container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info(
        "Microsoft SQL Server container started: %s", container.container_id[:12]
    )
    return container


def _stop_microsoft_sql(container: ManagedContainer) -> None:
    logger.info(
        "Stopping Microsoft SQL Server container: %s", container.container_id[:12]
    )
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Microsoft SQL Server container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _sqlcmd_invocation(*, user: str, password: str, database: str | None = None) -> str:
    args = [
        "-C",
        "-b",
        "-S",
        "127.0.0.1",
        "-U",
        user,
        "-P",
        password,
    ]
    if database is not None:
        args.extend(["-d", database])
    quoted_args = " ".join(shlex.quote(arg) for arg in args)
    return f"""
set -e
for candidate in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd sqlcmd; do
  if command -v "$candidate" >/dev/null 2>&1; then
    exec "$candidate" {quoted_args}
  fi
done
echo "sqlcmd not found" >&2
exit 127
"""


def _exec_microsoft_sql(
    container: ManagedContainer,
    *,
    user: str,
    password: str,
    sql: str,
    database: str | None = None,
) -> None:
    result = container.exec(
        [
            "/bin/bash",
            "-lc",
            _sqlcmd_invocation(user=user, password=password, database=database),
        ],
        input=sql,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)


def _wait_for_microsoft_sql(container: ManagedContainer, timeout: float) -> None:
    def _probe() -> tuple[bool, dict[str, str]]:
        if not container.is_running():
            return False, {"running": "false"}
        try:
            result = container.exec(
                [
                    "/bin/bash",
                    "-lc",
                    _sqlcmd_invocation(user="sa", password=MSSQL_SA_PASSWORD),
                ],
                input="SELECT 1;",
            )
        except ContainerCommandError as exc:
            logger.debug("SQL Server readiness probe command failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}
        if result.returncode == 0:
            return True, {"running": "true", "probe_returncode": "0"}
        return False, {
            "running": "true",
            "probe_returncode": str(result.returncode),
            "probe_stderr": result.stderr.strip(),
        }

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Microsoft SQL Server startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Microsoft SQL Server is ready")


def _create_test_data(container: ManagedContainer) -> None:
    sql = f"""
IF DB_ID(N'{MSSQL_DATABASE}') IS NULL
BEGIN
  CREATE DATABASE [{MSSQL_DATABASE}];
END;
GO
USE [{MSSQL_DATABASE}];
GO
IF SUSER_ID(N'{MSSQL_USER}') IS NULL
BEGIN
  CREATE LOGIN [{MSSQL_USER}] WITH PASSWORD = N'{MSSQL_PASSWORD}';
END;
IF USER_ID(N'{MSSQL_USER}') IS NULL
BEGIN
  CREATE USER [{MSSQL_USER}] FOR LOGIN [{MSSQL_USER}];
END;
ALTER ROLE db_datareader ADD MEMBER [{MSSQL_USER}];
ALTER ROLE db_datawriter ADD MEMBER [{MSSQL_USER}];
GO
DROP TABLE IF EXISTS dbo.types;
DROP TABLE IF EXISTS dbo.full_types;
DROP TABLE IF EXISTS dbo.large_values;
DROP TABLE IF EXISTS dbo.many_rows;
DROP TABLE IF EXISTS dbo.users;
GO
CREATE TABLE dbo.users (
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NULL,
  age INT NULL,
  is_active BIT NOT NULL
);
INSERT INTO dbo.users (id, name, email, age, is_active) VALUES
  (1, 'Alice', 'alice@example.com', 30, 1),
  (2, 'Bob', 'bob@example.com', 25, 1),
  (3, 'Charlie', NULL, 35, 0);
CREATE TABLE dbo.types (
  id INT NOT NULL PRIMARY KEY,
  tiny_val TINYINT NOT NULL,
  small_val SMALLINT NOT NULL,
  int_val INT NOT NULL,
  big_val BIGINT NOT NULL,
  bit_val BIT NOT NULL,
  real_val REAL NOT NULL,
  float_val FLOAT NOT NULL,
  decimal_val DECIMAL(10, 2) NOT NULL,
  numeric_val NUMERIC(12, 4) NOT NULL,
  money_val MONEY NOT NULL,
  smallmoney_val SMALLMONEY NOT NULL,
  guid_val UNIQUEIDENTIFIER NOT NULL,
  varchar_val VARCHAR(100) NOT NULL,
  nvarchar_val NVARCHAR(100) NOT NULL,
  binary_val VARBINARY(4) NOT NULL,
  nullable_val VARCHAR(50) NULL,
  datetime_val DATETIME NOT NULL
);
INSERT INTO dbo.types VALUES
  (1, 255, 32767, 2147483647, 9223372036854775807, 1, 3.5, 3.141592653589793,
   12345.67, -98765.4321, 123.4567, -12.3400,
   '6F9619FF-8B86-D011-B42D-00C04FC964FF',
   'hello', N'world', 0xDEADBEEF, NULL,
   CAST('2024-05-06T07:08:09.123' AS DATETIME));
CREATE TABLE dbo.full_types (
  id INT NOT NULL PRIMARY KEY,
  char_val CHAR(5) NOT NULL,
  nchar_val NCHAR(5) NOT NULL,
  text_val TEXT NOT NULL,
  ntext_val NTEXT NOT NULL,
  varchar_max_val VARCHAR(MAX) NOT NULL,
  nvarchar_max_val NVARCHAR(MAX) NOT NULL,
  binary_val BINARY(4) NOT NULL,
  varbinary_max_val VARBINARY(MAX) NOT NULL,
  image_val IMAGE NOT NULL,
  date_val DATE NOT NULL,
  time_val TIME(0) NOT NULL,
  smalldatetime_val SMALLDATETIME NOT NULL,
  datetime2_val DATETIME2(7) NOT NULL,
  datetimeoffset_val DATETIMEOFFSET(0) NOT NULL,
  nullable_int_val INT NULL,
  nullable_bit_val BIT NULL,
  nullable_float_val FLOAT NULL,
  nullable_money_val MONEY NULL,
  nullable_datetime_val DATETIME NULL,
  xml_val XML NOT NULL,
  json_val NVARCHAR(MAX) NOT NULL
);
INSERT INTO dbo.full_types VALUES
  (1, 'abc', N'äöü', 'legacy text', N'legacy ntext',
   CAST('varchar max' AS VARCHAR(MAX)),
   CAST(N'nvarchar max' AS NVARCHAR(MAX)),
   0x01020304,
   CAST(0xDEADBEEF AS VARBINARY(MAX)),
   CAST(0xAABBCC AS IMAGE),
   CAST('2024-05-06' AS DATE),
   CAST('00:00:10' AS TIME(0)),
   CAST('2024-05-06T07:08:00' AS SMALLDATETIME),
   CAST('2024-05-06T07:08:09.1234567' AS DATETIME2(7)),
   CAST('2024-05-06T07:08:09+02:00' AS DATETIMEOFFSET(0)),
   42, 1, 2.5, 99.9900,
   CAST('2024-05-06T07:08:09.123' AS DATETIME),
   CAST('<root><x>1</x></root>' AS XML),
   N'{{"x":1}}');
CREATE TABLE dbo.large_values (
  id INT NOT NULL PRIMARY KEY,
  payload VARCHAR(MAX) NOT NULL
);
INSERT INTO dbo.large_values VALUES
  (1, REPLICATE(CAST('x' AS VARCHAR(MAX)), 5000));
CREATE TABLE dbo.many_rows (
  id INT NOT NULL PRIMARY KEY
);
WITH numbers AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM numbers WHERE id < 10001
)
INSERT INTO dbo.many_rows
SELECT id FROM numbers
OPTION (MAXRECURSION 0);
"""
    logger.info("Creating Microsoft SQL Server test data")
    _exec_microsoft_sql(
        container,
        user="sa",
        password=MSSQL_SA_PASSWORD,
        sql=sql,
    )


def _prepare_live_stream_table(container: ManagedContainer) -> None:
    sql = f"""
USE [{MSSQL_DATABASE}];
GO
DROP TABLE IF EXISTS {LIVE_STREAM_TABLE};
GO
CREATE TABLE {LIVE_STREAM_TABLE} (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  payload VARCHAR(255) NOT NULL
);
"""
    _exec_microsoft_sql(
        container,
        user="sa",
        password=MSSQL_SA_PASSWORD,
        database=MSSQL_DATABASE,
        sql=sql,
    )


def _drop_live_stream_table(container: ManagedContainer) -> None:
    sql = f"""
USE [{MSSQL_DATABASE}];
GO
DROP TABLE IF EXISTS {LIVE_STREAM_TABLE};
"""
    _exec_microsoft_sql(
        container,
        user="sa",
        password=MSSQL_SA_PASSWORD,
        database=MSSQL_DATABASE,
        sql=sql,
    )


def _run_live_insert_loop(
    container: ManagedContainer,
    stop_event: threading.Event,
) -> None:
    if stop_event.wait(LIVE_STREAM_INITIAL_DELAY):
        return
    sql = f"""
INSERT INTO {LIVE_STREAM_TABLE} (payload) VALUES ('{LIVE_STREAM_TOKEN}');
"""
    while not stop_event.is_set():
        try:
            _exec_microsoft_sql(
                container,
                user=MSSQL_USER,
                password=MSSQL_PASSWORD,
                database=MSSQL_DATABASE,
                sql=sql,
            )
        except RuntimeError as exc:
            logger.warning("Live insert loop failed: %s", exc)
        if stop_event.wait(LIVE_STREAM_INSERT_INTERVAL):
            break


@fixture(options=MicrosoftSqlOptions)
def microsoft_sql() -> Iterator[dict[str, str]]:
    """Start Microsoft SQL Server and yield database connection variables."""
    opts = current_options("microsoft_sql")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    port = find_free_port()
    container: ManagedContainer | None = None
    stop_event = None
    inserter = None
    try:
        container = _start_microsoft_sql(runtime, port)
        _wait_for_microsoft_sql(container, STARTUP_TIMEOUT)
        _create_test_data(container)
        env = {
            "MSSQL_HOST": "127.0.0.1",
            "MSSQL_PORT": str(port),
            "MSSQL_USER": MSSQL_USER,
            "MSSQL_PASSWORD": MSSQL_PASSWORD,
            "MSSQL_DATABASE": MSSQL_DATABASE,
            "MSSQL_CONTAINER_ID": container.container_id,
            "MSSQL_CONTAINER_RUNTIME": runtime.binary,
        }
        if opts.streaming:
            _prepare_live_stream_table(container)
            stop_event = threading.Event()
            inserter = threading.Thread(
                target=_run_live_insert_loop,
                args=(container, stop_event),
                daemon=True,
            )
            inserter.start()
            env.update(
                {
                    "MSSQL_LIVE_STREAM_TABLE": LIVE_STREAM_TABLE,
                    "MSSQL_LIVE_STREAM_TOKEN": LIVE_STREAM_TOKEN,
                }
            )
        yield env
    finally:
        if stop_event is not None:
            stop_event.set()
        if inserter is not None:
            inserter.join(timeout=5)
            if inserter.is_alive():
                logger.warning("Live insert loop thread did not stop in time")
        if stop_event is not None and container is not None:
            try:
                _drop_live_stream_table(container)
            except RuntimeError as exc:
                logger.warning("Failed to drop live stream table: %s", exc)
        if container is not None:
            _stop_microsoft_sql(container)
