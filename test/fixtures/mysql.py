"""MySQL fixture for database integration testing.

Provides a MySQL server instance for testing the from_mysql operator.

Environment variables yielded:
- MYSQL_HOST: Server hostname (127.0.0.1)
- MYSQL_PORT: Server port (dynamically allocated)
- MYSQL_USER: Test username
- MYSQL_PASSWORD: Test password
- MYSQL_DATABASE: Pre-created test database
- MYSQL_ROOT_PASSWORD: Root password for admin operations
- MYSQL_TLS_CAFILE: Path to CA certificate (when tls=True)
- MYSQL_TLS_CERTFILE: Path to client certificate (when tls=True)
- MYSQL_TLS_KEYFILE: Path to client private key (when tls=True)
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
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

# MySQL configuration
MYSQL_IMAGE = "mysql:8.0"
MYSQL_ROOT_PASSWORD = "root_test_password"
MYSQL_USER = "tenzir"
MYSQL_PASSWORD = "tenzir_test_password"
MYSQL_DATABASE = "tenzir_test"
STARTUP_TIMEOUT = 120  # seconds (MySQL can be slow to start)
HEALTH_CHECK_INTERVAL = 2  # seconds
LIVE_STREAM_TABLE = "users_live_stream"
LIVE_STREAM_TOKEN = "live-stream-token"
LIVE_STREAM_INITIAL_DELAY = 5.0
LIVE_STREAM_INSERT_INTERVAL = 0.1
LIVE_SIGNED_STREAM_TABLE = "users_live_signed_stream"
LIVE_SIGNED_STREAM_TOKEN = "live-signed-stream-token"
LIVE_SIGNED_STREAM_SEED_TOKEN = "live-signed-seed-token"
LIVE_SIGNED_STREAM_START_ID = 0


@dataclass(frozen=True)
class MysqlOptions:
    tls: bool = True
    streaming: str = ""  # "unsigned" or "signed"; empty = disabled


def _start_mysql(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    """Start MySQL container and return a managed container."""
    container_name = f"tenzir-test-mysql-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:3306",
        "-e",
        f"MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASSWORD}",
        "-e",
        f"MYSQL_USER={MYSQL_USER}",
        "-e",
        f"MYSQL_PASSWORD={MYSQL_PASSWORD}",
        "-e",
        f"MYSQL_DATABASE={MYSQL_DATABASE}",
        MYSQL_IMAGE,
        "--default-authentication-plugin=mysql_native_password",
    ]
    logger.info("Starting MySQL container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("MySQL container started: %s", container.container_id[:12])
    return container


def _stop_mysql(container: ManagedContainer) -> None:
    """Stop and remove MySQL container."""
    logger.info("Stopping MySQL container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop MySQL container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_mysql(container: ManagedContainer, timeout: float) -> None:
    """Wait for MySQL to become ready."""
    def _probe() -> tuple[bool, dict[str, str]]:
        running = container.is_running()
        if not running:
            logger.debug("Container not running yet")
            return False, {"running": "false"}
        try:
            result = container.exec(
                [
                    "mysqladmin",
                    "ping",
                    "-h",
                    "127.0.0.1",
                    "-uroot",
                    f"-p{MYSQL_ROOT_PASSWORD}",
                    "--silent",
                ]
            )
        except ContainerCommandError as exc:
            logger.debug("MySQL readiness probe command failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}
        if result.returncode == 0:
            return True, {"running": "true", "probe_returncode": "0"}
        stderr = result.stderr.strip()
        logger.debug("MySQL not ready yet: %s", stderr)
        return False, {
            "running": "true",
            "probe_returncode": str(result.returncode),
            "probe_stderr": stderr,
        }

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="MySQL startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("MySQL is ready")
    # Give extra time for full initialization
    time.sleep(5)


def _create_test_data(container: ManagedContainer) -> None:
    """Create test tables and data in the MySQL database."""
    # Grant privileges to the test user first, then create tables
    sql = f"""
    GRANT ALL PRIVILEGES ON {MYSQL_DATABASE}.* TO '{MYSQL_USER}'@'%';
    FLUSH PRIVILEGES;
    USE {MYSQL_DATABASE};
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255),
        age INT,
        balance DECIMAL(10, 2),
        is_active TINYINT(1) DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        metadata JSON
    );
    INSERT INTO users (name, email, age, balance, is_active, metadata) VALUES
        ('Alice', 'alice@example.com', 30, 1000.50, 1, '{{"role": "admin"}}'),
        ('Bob', 'bob@example.com', 25, 500.00, 1, '{{"role": "user"}}'),
        ('Charlie', 'charlie@example.com', 35, 750.25, 0, '{{"role": "user"}}');
    CREATE TABLE IF NOT EXISTS events (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_type VARCHAR(50) NOT NULL,
        severity ENUM('low', 'medium', 'high', 'critical') DEFAULT 'low',
        message TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_ip VARCHAR(45)
    );
    INSERT INTO events (event_type, severity, message, source_ip) VALUES
        ('login', 'low', 'User login successful', '192.168.1.100'),
        ('error', 'high', 'Database connection failed', '10.0.0.50'),
        ('alert', 'critical', 'Security breach detected', '172.16.0.1');
    CREATE TABLE IF NOT EXISTS numbers (
        tiny_val TINYINT,
        small_val SMALLINT,
        int_val INT,
        big_val BIGINT,
        ubig_val BIGINT UNSIGNED,
        float_val FLOAT,
        double_val DOUBLE,
        decimal_val DECIMAL(15, 5)
    );
    INSERT INTO numbers VALUES
        (127, 32767, 2147483647, 9223372036854775807, 18446744073709551615,
         3.14, 3.141592653589793, 12345.67890);
    CREATE TABLE IF NOT EXISTS types (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tiny_val TINYINT,
        ubig_val BIGINT UNSIGNED,
        float_val DOUBLE,
        decimal_val DECIMAL(10,2),
        str_val VARCHAR(100),
        text_val TEXT,
        blob_val BLOB,
        date_val DATE,
        datetime_val DATETIME,
        json_val JSON,
        enum_val ENUM('a','b','c'),
        nullable_val VARCHAR(50)
    );
    INSERT INTO types (id, tiny_val, ubig_val, float_val, decimal_val, str_val,
                       text_val, blob_val, date_val, datetime_val, json_val,
                       enum_val, nullable_val) VALUES
        (1, -128, 18446744073709551615, 3.14, 99.99, 'hello', 'text',
         X'DEADBEEF', '2025-01-15', '2025-01-15 14:30:00',
         '{{"key":"value"}}', 'b', 'not null'),
        (2, 127, 0, 0.0, 0.00, '', '', X'',
         '1970-01-01', '1970-01-01 00:00:00', '[]', 'a', NULL);
    CREATE TABLE IF NOT EXISTS large_packets (
        id INT PRIMARY KEY,
        payload LONGTEXT NOT NULL
    );
    INSERT INTO large_packets (id, payload) VALUES
        (1, REPEAT('x', 17000000)),
        (2, 'tail');
    """
    logger.info("Creating test tables and data")
    # Use root to create tables and grant privileges
    # Use TCP connection (-h 127.0.0.1) instead of socket
    result = container.exec(
        [
            "mysql",
            "-h",
            "127.0.0.1",
            "-uroot",
            f"-p{MYSQL_ROOT_PASSWORD}",
        ],
        input=sql,
    )
    if result.returncode != 0:
        logger.error("Failed to create test data: %s", result.stderr)
        raise RuntimeError(f"Failed to create test data: {result.stderr}")
    logger.info("Test data created successfully")


def _extract_tls_files(container: ManagedContainer, dest_dir: str) -> dict[str, str]:
    """Extract TLS certificates and keys from the MySQL container.

    MySQL 8 auto-generates several TLS files in /var/lib/mysql/:
    - ca.pem: CA certificate
    - client-cert.pem: Client certificate (signed by the CA)
    - client-key.pem: Client private key

    Returns a dict mapping descriptive names to local file paths.
    """
    files = {
        "ca": "ca.pem",
        "client_cert": "client-cert.pem",
        "client_key": "client-key.pem",
    }
    paths = {}
    for name, filename in files.items():
        local_path = str(Path(dest_dir) / filename)
        result = container.copy_from(f"/var/lib/mysql/{filename}", local_path, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to extract {filename}: {result.stderr}")
        logger.info("Extracted %s to %s", filename, local_path)
        paths[name] = local_path
    return paths


def _exec_mysql_sql(
    container: ManagedContainer,
    *,
    user: str,
    password: str,
    sql: str,
    database: str | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        "mysql",
        "-h",
        "127.0.0.1",
        f"-u{user}",
        f"-p{password}",
    ]
    if database is not None:
        cmd.extend(["-D", database])
    return container.exec(cmd, input=sql)


def _prepare_live_stream_table(container: ManagedContainer) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {LIVE_STREAM_TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        payload VARCHAR(255) NOT NULL
    );
    TRUNCATE TABLE {LIVE_STREAM_TABLE};
    """
    result = _exec_mysql_sql(
        container,
        user="root",
        password=MYSQL_ROOT_PASSWORD,
        database=MYSQL_DATABASE,
        sql=sql,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to prepare live stream table: {result.stderr}")


def _prepare_live_signed_stream_table(container: ManagedContainer) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {LIVE_SIGNED_STREAM_TABLE} (
        id BIGINT PRIMARY KEY,
        payload VARCHAR(255) NOT NULL
    );
    TRUNCATE TABLE {LIVE_SIGNED_STREAM_TABLE};
    INSERT INTO {LIVE_SIGNED_STREAM_TABLE} (id, payload) VALUES
        (-2, '{LIVE_SIGNED_STREAM_SEED_TOKEN}'),
        (-1, '{LIVE_SIGNED_STREAM_SEED_TOKEN}');
    """
    result = _exec_mysql_sql(
        container,
        user="root",
        password=MYSQL_ROOT_PASSWORD,
        database=MYSQL_DATABASE,
        sql=sql,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to prepare signed live stream table: {result.stderr}"
        )


def _drop_table(container: ManagedContainer, table_name: str) -> None:
    result = _exec_mysql_sql(
        container,
        user="root",
        password=MYSQL_ROOT_PASSWORD,
        database=MYSQL_DATABASE,
        sql=f"DROP TABLE IF EXISTS {table_name};",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to drop table {table_name}: {result.stderr}")


def _run_live_insert_loop(
    container: ManagedContainer,
    stop_event: threading.Event,
) -> None:
    if stop_event.wait(LIVE_STREAM_INITIAL_DELAY):
        return
    sql = f"INSERT INTO {LIVE_STREAM_TABLE} (payload) VALUES ('{LIVE_STREAM_TOKEN}');"
    while not stop_event.is_set():
        result = _exec_mysql_sql(
            container,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            sql=sql,
        )
        if result.returncode != 0:
            logger.warning("Live insert loop failed: %s", result.stderr.strip())
        if stop_event.wait(LIVE_STREAM_INSERT_INTERVAL):
            break


def _run_live_signed_insert_loop(
    container: ManagedContainer,
    stop_event: threading.Event,
) -> None:
    if stop_event.wait(LIVE_STREAM_INITIAL_DELAY):
        return
    next_id = LIVE_SIGNED_STREAM_START_ID
    while not stop_event.is_set():
        sql = (
            "INSERT INTO "
            f"{LIVE_SIGNED_STREAM_TABLE} (id, payload) VALUES "
            f"({next_id}, '{LIVE_SIGNED_STREAM_TOKEN}');"
        )
        result = _exec_mysql_sql(
            container,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            sql=sql,
        )
        if result.returncode != 0:
            logger.warning("Signed live insert loop failed: %s", result.stderr.strip())
        next_id += 1
        if stop_event.wait(LIVE_STREAM_INSERT_INTERVAL):
            break


@fixture(options=MysqlOptions)
def mysql() -> Iterator[dict[str, str]]:
    """Start MySQL and yield environment variables for database access."""
    opts = current_options("mysql")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    port = find_free_port()
    container: ManagedContainer | None = None
    tmp_dir = tempfile.mkdtemp(prefix="tenzir-mysql-tls-") if opts.tls else None
    stop_event = None
    inserter = None
    stream_table = None
    try:
        container = _start_mysql(runtime, port)
        _wait_for_mysql(container, STARTUP_TIMEOUT)
        _create_test_data(container)
        env: dict[str, str] = {
            "MYSQL_HOST": "127.0.0.1",
            "MYSQL_PORT": str(port),
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PASSWORD,
            "MYSQL_ROOT_PASSWORD": MYSQL_ROOT_PASSWORD,
            "MYSQL_DATABASE": MYSQL_DATABASE,
            "MYSQL_CONTAINER_ID": container.container_id,
            "MYSQL_CONTAINER_RUNTIME": runtime.binary,
        }
        if opts.tls:
            assert tmp_dir is not None
            tls_files = _extract_tls_files(container, tmp_dir)
            env.update(
                {
                    "MYSQL_TLS_CAFILE": tls_files["ca"],
                    "MYSQL_TLS_CERTFILE": tls_files["client_cert"],
                    "MYSQL_TLS_KEYFILE": tls_files["client_key"],
                }
            )
        if opts.streaming == "unsigned":
            _prepare_live_stream_table(container)
            stream_table = LIVE_STREAM_TABLE
            stop_event = threading.Event()
            inserter = threading.Thread(
                target=_run_live_insert_loop,
                args=(container, stop_event),
                daemon=True,
            )
            inserter.start()
            env.update(
                {
                    "MYSQL_LIVE_STREAM_TABLE": LIVE_STREAM_TABLE,
                    "MYSQL_LIVE_STREAM_TOKEN": LIVE_STREAM_TOKEN,
                }
            )
        elif opts.streaming == "signed":
            _prepare_live_signed_stream_table(container)
            stream_table = LIVE_SIGNED_STREAM_TABLE
            stop_event = threading.Event()
            inserter = threading.Thread(
                target=_run_live_signed_insert_loop,
                args=(container, stop_event),
                daemon=True,
            )
            inserter.start()
            env.update(
                {
                    "MYSQL_LIVE_SIGNED_STREAM_TABLE": LIVE_SIGNED_STREAM_TABLE,
                    "MYSQL_LIVE_SIGNED_STREAM_TOKEN": LIVE_SIGNED_STREAM_TOKEN,
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
        if stream_table is not None and container is not None:
            try:
                _drop_table(container, stream_table)
            except RuntimeError as exc:
                logger.warning("Failed to drop stream table: %s", exc)
        if container is not None:
            _stop_mysql(container)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
