# runner: python
"""Verify the MySQL fixture starts correctly and has test data."""

import os
import subprocess


def main() -> None:
    """Connect to MySQL and verify test data exists."""
    runtime = os.environ.get("MYSQL_CONTAINER_RUNTIME")
    container_id = os.environ.get("MYSQL_CONTAINER_ID")
    if not runtime or not container_id:
        print("MySQL container info not available")
        return
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    database = os.environ["MYSQL_DATABASE"]
    query = "SELECT name FROM users ORDER BY name;"
    result = subprocess.run(
        [
            runtime,
            "exec",
            container_id,
            "mysql",
            "-h",
            "127.0.0.1",
            f"-u{user}",
            f"-p{password}",
            database,
            "-N",
            "-e",
            query,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.strip().split("\n"):
        if line and not line.startswith("mysql:"):
            print(line)


if __name__ == "__main__":
    main()
