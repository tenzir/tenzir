# runner: python
"""Seed a ClickHouse table with special scalar types."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
DROP TABLE IF EXISTS fc_special_types;
CREATE TABLE fc_special_types (
  id UInt64,
  addr4 IPv4,
  addr6 IPv6,
  uid UUID,
  dec Decimal64(2),
  stamp DateTime64(3),
  status Enum8('low' = 1, 'high' = 2)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO fc_special_types VALUES (
  1,
  '10.0.0.1',
  '2001:db8::1',
  '01234567-89ab-cdef-0123-456789abcdef',
  123.45,
  '2025-01-15 12:34:56.789',
  'high'
);
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
