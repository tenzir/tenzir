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
            "SELECT count(*) AS total, "
            "count(*) FILTER (rec IS NULL) AS null_records, "
            "count(*) FILTER (rec.host IS NOT NULL) AS hosts, "
            "count(*) FILTER (rec IS NULL AND (rec.host IS NOT NULL "
            "OR rec.ports IS NOT NULL)) AS leaked FROM structs;",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
