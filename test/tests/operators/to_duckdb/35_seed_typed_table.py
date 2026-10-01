# runner: python

"""Creates a table with column types that TQL has no counterpart for."""

import os
import subprocess


def main() -> None:
    subprocess.run(
        [
            os.environ["DUCKDB_CLI"],
            os.path.join(os.environ["DUCKDB_ROOT"], "out.duckdb"),
            "-c",
            "CREATE TABLE typed (id UUID, mood ENUM('happy', 'sad'), "
            "amount DECIMAL(6, 2), big UHUGEINT);",
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
