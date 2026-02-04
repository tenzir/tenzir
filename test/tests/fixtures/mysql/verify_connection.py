# runner: python
"""Verify the MySQL fixture starts correctly and has test data."""

import os
import shutil
import subprocess


def find_container_runtime() -> str | None:
    """Find an available container runtime."""
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def main() -> None:
    """Connect to MySQL and verify test data exists."""
    runtime = find_container_runtime()
    if runtime is None:
        print("No container runtime found")
        return
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    database = os.environ["MYSQL_DATABASE"]
    # Find the running MySQL container
    result = subprocess.run(
        [runtime, "ps", "-q", "-f", "name=tenzir-test-mysql"],
        capture_output=True,
        text=True,
        check=True,
    )
    container_id = result.stdout.strip()
    if not container_id:
        print("MySQL container not found")
        return
    # Query the database using docker exec
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
    # Print names one per line (filter out warnings)
    for line in result.stdout.strip().split("\n"):
        if line and not line.startswith("mysql:"):
            print(line)


if __name__ == "__main__":
    main()
