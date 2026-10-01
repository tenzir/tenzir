# runner: python

import os
import subprocess


def main() -> None:
    result = subprocess.run(
        [
            os.environ["DUCKDB_CLI"],
            os.path.join(os.environ["DUCKDB_ROOT"], "out.duckdb"),
            "-readonly",
            "-json",
            "-c",
            "SELECT i, s, u, list FROM types WHERE i > 5 ORDER BY i;",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
