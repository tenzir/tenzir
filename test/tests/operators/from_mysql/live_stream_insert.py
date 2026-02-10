# runner: python
"""Validate that from_mysql live mode streams a new insert."""

import os
import shutil
import subprocess
import tempfile
import time

import pymysql

# /// script
# dependencies = ["pymysql"]
# ///


def main() -> None:
    tenzir = shutil.which("tenzir")
    if tenzir is None:
        raise RuntimeError("`tenzir` binary not found in PATH")

    table_name = "users_live_stream"
    token = "live-stream-token"

    conn = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  payload VARCHAR(255) NOT NULL
                )
                """
            )
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()

    pipeline = "\n".join(
        [
            f'from_mysql table="{table_name}",',
            '           live=true,',
            '           tracking_column="id",',
            f'           host="{os.environ["MYSQL_HOST"]}",',
            f'           port={int(os.environ["MYSQL_PORT"])},',
            f'           user="{os.environ["MYSQL_USER"]}",',
            f'           password="{os.environ["MYSQL_PASSWORD"]}",',
            f'           database="{os.environ["MYSQL_DATABASE"]}"',
            "head 1",
        ]
    )
    with tempfile.NamedTemporaryFile("w", suffix=".tql", delete=False) as f:
        f.write(pipeline)
        pipeline_file = f.name
    process = subprocess.Popen(
        [
            tenzir,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            "--neo",
            "-f",
            pipeline_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give the source enough time to initialize its high-water mark.
        time.sleep(1.5)
        conn = pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.environ["MYSQL_PORT"]),
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=os.environ["MYSQL_DATABASE"],
        )
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {table_name} (payload) VALUES (%s)", (token,)
                )
                conn.commit()
        stdout, stderr = process.communicate(timeout=20)
    except BaseException:
        process.kill()
        process.communicate()
        raise
    finally:
        os.unlink(pipeline_file)
        cleanup = pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.environ["MYSQL_PORT"]),
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=os.environ["MYSQL_DATABASE"],
        )
        with cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                cleanup.commit()

    if process.returncode != 0:
        raise RuntimeError(
            f"tenzir exited with {process.returncode}: stdout={stdout!r} stderr={stderr!r}"
        )
    if token not in stdout:
        raise RuntimeError(f"did not find inserted token {token!r} in output: {stdout!r}")

    print(stdout.strip())


if __name__ == "__main__":
    main()
