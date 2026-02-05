"""MySQL fixture for database integration testing.

Provides a MySQL server instance for testing the from_mysql operator.

Environment variables yielded:
- MYSQL_HOST: Server hostname (127.0.0.1)
- MYSQL_PORT: Server port (dynamically allocated)
- MYSQL_USER: Test username
- MYSQL_PASSWORD: Test password
- MYSQL_DATABASE: Pre-created test database
- MYSQL_ROOT_PASSWORD: Root password for admin operations
"""

from __future__ import annotations

import logging
import shutil
import socket
import subprocess
import time
import uuid
from typing import Iterator

from tenzir_test import fixture

logger = logging.getLogger(__name__)

# MySQL configuration
MYSQL_IMAGE = "mysql:8.0"
MYSQL_ROOT_PASSWORD = "root_test_password"
MYSQL_USER = "tenzir"
MYSQL_PASSWORD = "tenzir_test_password"
MYSQL_DATABASE = "tenzir_test"
STARTUP_TIMEOUT = 120  # seconds (MySQL can be slow to start)
HEALTH_CHECK_INTERVAL = 2  # seconds


def _find_free_port() -> int:
    """Find an available port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _find_container_runtime() -> str | None:
    """Find an available container runtime (podman or docker)."""
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def _start_mysql(runtime: str, port: int) -> str:
    """Start MySQL container and return container ID."""
    container_name = f"tenzir-test-mysql-{uuid.uuid4().hex[:8]}"
    cmd = [
        runtime,
        "run",
        "-d",
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
    logger.info("Starting MySQL container with %s: %s", runtime, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()
    logger.info("MySQL container started: %s", container_id[:12])
    return container_id


def _stop_mysql(runtime: str, container_id: str) -> None:
    """Stop and remove MySQL container."""
    logger.info("Stopping MySQL container: %s", container_id[:12])
    subprocess.run(
        [runtime, "stop", container_id],
        capture_output=True,
        check=False,
    )


def _wait_for_mysql(runtime: str, container_id: str, timeout: float) -> bool:
    """Wait for MySQL to become ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            [runtime, "inspect", "-f", "{{.State.Running}}", container_id],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or result.stdout.strip() != "true":
            logger.debug("Container not running yet")
            time.sleep(HEALTH_CHECK_INTERVAL)
            continue
        # Use root to check if MySQL is ready (user may not be created yet)
        # Use TCP connection (127.0.0.1) instead of socket
        result = subprocess.run(
            [
                runtime,
                "exec",
                container_id,
                "mysqladmin",
                "ping",
                "-h",
                "127.0.0.1",
                "-uroot",
                f"-p{MYSQL_ROOT_PASSWORD}",
                "--silent",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            logger.info("MySQL is ready")
            # Give extra time for full initialization
            time.sleep(5)
            return True
        logger.debug("MySQL not ready yet: %s", result.stderr.strip())
        time.sleep(HEALTH_CHECK_INTERVAL)
    return False


def _create_test_data(runtime: str, container_id: str) -> None:
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
    """
    logger.info("Creating test tables and data")
    # Use root to create tables and grant privileges
    # Use TCP connection (-h 127.0.0.1) instead of socket
    result = subprocess.run(
        [
            runtime,
            "exec",
            "-i",
            container_id,
            "mysql",
            "-h",
            "127.0.0.1",
            "-uroot",
            f"-p{MYSQL_ROOT_PASSWORD}",
        ],
        input=sql,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        logger.error("Failed to create test data: %s", result.stderr)
        raise RuntimeError(f"Failed to create test data: {result.stderr}")
    logger.info("Test data created successfully")


@fixture()
def mysql() -> Iterator[dict[str, str]]:
    """Start MySQL and yield environment variables for database access."""
    runtime = _find_container_runtime()
    if runtime is None:
        raise RuntimeError(
            "A container runtime (podman or docker) is required for MySQL "
            "fixture but none was found."
        )
    port = _find_free_port()
    container_id = None
    try:
        container_id = _start_mysql(runtime, port)
        if not _wait_for_mysql(runtime, container_id, STARTUP_TIMEOUT):
            raise RuntimeError(
                f"MySQL failed to start within {STARTUP_TIMEOUT} seconds"
            )
        _create_test_data(runtime, container_id)
        yield {
            "MYSQL_HOST": "127.0.0.1",
            "MYSQL_PORT": str(port),
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PASSWORD,
            "MYSQL_ROOT_PASSWORD": MYSQL_ROOT_PASSWORD,
            "MYSQL_DATABASE": MYSQL_DATABASE,
        }
    finally:
        if container_id:
            _stop_mysql(runtime, container_id)
