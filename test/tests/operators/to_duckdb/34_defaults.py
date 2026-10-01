# runner: python

"""Columns without a field receive their `DEFAULT` value, and generated
columns compute theirs."""

import os
import shlex
import subprocess


def main() -> None:
    db = os.path.join(os.environ["DUCKDB_ROOT"], "defaults.duckdb")
    cli = os.environ["DUCKDB_CLI"]
    subprocess.run(
        [
            cli,
            db,
            "-c",
            "CREATE TABLE t (a BIGINT, doubled BIGINT GENERATED ALWAYS AS "
            "(a * 2) VIRTUAL, c BIGINT NOT NULL DEFAULT 5);",
        ],
        check=True,
    )
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
    subprocess.run(
        [
            *binary,
            "--nova",
            "--bare-mode",
            "--console-verbosity=warning",
            f'from {{a: 1}}, {{a: 2, c: 7}}\nto_duckdb "{db}", table="t"',
        ],
        check=True,
    )
    result = subprocess.run(
        [cli, db, "-readonly", "-json", "-c", "SELECT * FROM t ORDER BY a;"],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="")


if __name__ == "__main__":
    main()
