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
            "SELECT id::VARCHAR AS id, mood::VARCHAR AS mood, "
            "amount::VARCHAR AS amount, big::VARCHAR AS big FROM typed "
            "ORDER BY id NULLS LAST;",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
