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
            "DESCRIBE lenient; SELECT * FROM lenient ORDER BY s;",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
