# runner: python
# timeout: 60

"""Resolve primary keys using DuckDB's identifier rules, including qualifiers."""

import json
import os
import shlex
import subprocess


def main() -> None:
    db = os.path.join(os.environ["DUCKDB_ROOT"], "CaseDb.duckdb")
    subprocess.run(
        [
            os.environ["DUCKDB_CLI"],
            db,
            "-c",
            'CREATE SCHEMA "MiXeD"; '
            'CREATE TABLE "MiXeD"."KeYeD" (id INTEGER PRIMARY KEY); '
            'INSERT INTO "MiXeD"."KeYeD" VALUES (1); '
            "CREATE TABLE composite (a INTEGER, b INTEGER, PRIMARY KEY(a, b)); "
            "INSERT INTO composite VALUES (1, 2); "
            "CREATE TABLE only_unique (id INTEGER UNIQUE); "
            "INSERT INTO only_unique VALUES (1); "
            "CREATE TABLE text_key (id VARCHAR PRIMARY KEY); "
            "INSERT INTO text_key VALUES ('one');",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])

    def read(table: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                *binary,
                "--nova",
                "--bare-mode",
                "--console-verbosity=warning",
                f'from_duckdb "{db}", table="{table}", live=true | head 1 | write_json',
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

    for table in (
        "mixed.keyed",
        "MIXED.KEYED",
        "casedb.mixed.keyed",
        "CASEDB.MIXED.KEYED",
    ):
        result = read(table)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"id": 1}, result.stdout
        print(f"{table}: primary key resolved")

    for table in ("composite", "only_unique", "text_key"):
        result = read(table)
        assert result.returncode != 0, result.stdout
        assert "no single-column integer primary key" in result.stderr, result.stderr
        print(f"{table}: primary key rejected")


if __name__ == "__main__":
    main()
