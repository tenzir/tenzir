# runner: python
"""Keep a writer alive across schema rejection, idle refresh, and marker removal."""

import json
import os
import shlex
import subprocess
import time

# /// script
# ///


def query(sql: str) -> str:
    return subprocess.run(
        [
            os.environ["CLICKHOUSE_CONTAINER_RUNTIME"],
            "exec",
            os.environ["CLICKHOUSE_CONTAINER_ID"],
            "clickhouse-client",
            f"--password={os.environ['CLICKHOUSE_PASSWORD']}",
            "--multiquery",
            "--query",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def wait_for(predicate, writer: subprocess.Popen, description: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if predicate():
            return
        if writer.poll() is not None:
            raise AssertionError(writer.stderr.read())
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {description}")


def main() -> None:
    query("""
CREATE TABLE sa_live (
  id UInt64, old UInt32, extra JSON COMMENT 'tenzir:catch_all'
) ENGINE=MergeTree ORDER BY id;
""")
    pipeline = """
from_stdin { read_ndjson }
id = uint(id)
old = uint(old)
fresh = uint(fresh)
later = uint(later)
to_clickhouse table="sa_live", host=env("CLICKHOUSE_HOST"),
  port=int(env("CLICKHOUSE_PORT")), password=env("CLICKHOUSE_PASSWORD"),
  tls=false, mode="append", max_batch_rows=1, batch_timeout=20ms
"""
    writer = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    def send(value: dict) -> None:
        writer.stdin.write(json.dumps(value) + "\n")
        writer.stdin.flush()

    try:
        send({"id": 1, "old": 1, "fresh": 7, "later": 70})
        wait_for(
            lambda: query("SELECT count() FROM sa_live") == "1", writer, "first insert"
        )
        query("ALTER TABLE sa_live DROP COLUMN old, ADD COLUMN fresh UInt32")
        # The old mapping receives a definite server rejection. Reloading and
        # retrying must also pick up fresh, without replaying acknowledged id=1.
        send({"id": 2, "old": 2, "fresh": 8, "later": 80})
        wait_for(
            lambda: query("SELECT count() FROM sa_live") == "2", writer, "schema retry"
        )
        rows = [
            json.loads(x)
            for x in query(
                "SELECT * FROM sa_live ORDER BY id FORMAT JSONEachRow"
            ).splitlines()
        ]
        assert rows[0]["extra"] == {"fresh": 7, "later": 70}, rows
        assert rows[1]["fresh"] == 8 and rows[1]["extra"] == {"old": 2, "later": 80}, (
            rows
        )

        # Observe the actual maintenance DESCRIBE while there is no input.
        # SQL query-log polling avoids a fixed sleep at the refresh boundary.
        query("SYSTEM FLUSH LOGS")
        count_sql = """SELECT count() FROM system.query_log
WHERE type='QueryFinish' AND query LIKE 'DESCRIBE TABLE sa_live %'"""
        previous = int(query(count_sql))
        query("ALTER TABLE sa_live ADD COLUMN later UInt32")

        def refreshed() -> bool:
            query("SYSTEM FLUSH LOGS")
            return int(query(count_sql)) > previous

        wait_for(refreshed, writer, "idle schema refresh")
        send({"id": 3, "old": 3, "fresh": 9, "later": 99})
        wait_for(
            lambda: query("SELECT count() FROM sa_live") == "3",
            writer,
            "post-refresh insert",
        )
        assert query("SELECT later FROM sa_live WHERE id=3") == "99"

        # Marker removal must fail refresh. Dropping a mapped column makes the
        # next write reject immediately rather than waiting another interval.
        query("ALTER TABLE sa_live COMMENT COLUMN extra '', DROP COLUMN fresh")
        send({"id": 4, "old": 4, "fresh": 10, "later": 100})
        writer.stdin.close()
        writer.wait(timeout=60)
        stderr = writer.stderr.read()
        assert writer.returncode != 0, stderr
        assert "catch-all marker was removed" in stderr, stderr
        assert query("SELECT count() FROM sa_live") == "3"
        assert query("SELECT count() FROM sa_live WHERE id=1") == "1"
    finally:
        if writer.poll() is None:
            writer.terminate()
            try:
                writer.wait(timeout=5)
            except subprocess.TimeoutExpired:
                writer.kill()
                writer.wait()
    print("ok")


if __name__ == "__main__":
    main()
