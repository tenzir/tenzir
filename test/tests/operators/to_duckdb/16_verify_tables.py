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
            "DESCRIBE keyed; SELECT * FROM keyed ORDER BY id;"
            "DESCRIBE nulls; SELECT * FROM nulls ORDER BY id;"
            "SELECT count(*) AS n, min(i) AS lo, max(i) AS hi, sum(i) AS total"
            " FROM filtered;",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
