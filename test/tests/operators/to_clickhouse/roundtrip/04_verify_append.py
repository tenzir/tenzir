# runner: python
"""Verify 20k rows after append (10k create + 10k append)."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    result = subprocess.run(
        [runtime, "exec", container, "clickhouse-client",
         f"--password={password}",
         "--query=SELECT count() FROM test_basic"],
        capture_output=True, text=True, check=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
