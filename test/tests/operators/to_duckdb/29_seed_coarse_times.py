# runner: python

import os
import subprocess


def main() -> None:
    subprocess.run(
        [
            os.environ["DUCKDB_CLI"],
            os.path.join(os.environ["DUCKDB_ROOT"], "out.duckdb"),
            "-c",
            "CREATE TABLE coarse_times (t TIMESTAMP, t_s TIMESTAMP_S);",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


if __name__ == "__main__":
    main()
