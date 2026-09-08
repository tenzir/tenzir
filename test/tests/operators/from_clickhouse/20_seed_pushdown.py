# runner: python
"""Seed a ClickHouse table that exercises filter, limit, and projection pushdown."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
DROP TABLE IF EXISTS fc_pushdown;
CREATE TABLE fc_pushdown (
  id UInt64,
  x Int64,
  y String,
  z UInt8,
  flag Bool,
  n Nullable(Int64),
  s Nullable(String),
  meta Tuple(source String, level Nullable(Int64)),
  status Enum8('low' = 1, 'high' = 2),
  f Float64
) ENGINE = MergeTree ORDER BY id;
INSERT INTO fc_pushdown VALUES
(1, -5, 'foo', 1, true,  NULL, NULL,  ('a', 1),    'low',  0.5),
(2,  0, 'bar', 2, false, 1,    'a',   ('a', NULL), 'high', 1.5),
(3,  7, 'foo', 3, true,  2,    'b',   ('b', 2),    'low',  2.5),
(4,  9, 'baz', 9, false, NULL, 'foo', ('b', 3),    'high', 3.5),
(5, 11, 'qux', 2, true,  5,    NULL,  ('c', 4),    'low',  4.5);
"""
    subprocess.run(
        [
            runtime,
            "exec",
            container,
            "clickhouse-client",
            f"--password={password}",
            "--multiquery",
            f"--query={sql}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )


if __name__ == "__main__":
    main()
