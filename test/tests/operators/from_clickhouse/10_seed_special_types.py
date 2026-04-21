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
  status Enum8('low' = 1, 'high' = 2),
  tm Time,
  tm64 Time64(3),
  tm_nullable Nullable(Time),
  tm64_nullable Nullable(Time64(3)),
  tm_lc LowCardinality(Time),
  tm64_lc LowCardinality(Time64(3))
) ENGINE = MergeTree ORDER BY id;
INSERT INTO fc_special_types VALUES
(
  1,
  '10.0.0.1',
  '2001:db8::1',
  '01234567-89ab-cdef-0123-456789abcdef',
  123.45,
  '2025-01-15 12:34:56.789',
  'high',
  '00:00:42',
  '00:00:01.234',
  '00:00:07',
  '00:00:00.456',
  '00:00:03',
  '00:00:00.789'
),
(
  2,
  '10.0.0.2',
  '2001:db8::2',
  'fedcba98-7654-3210-fedc-ba9876543210',
  67.89,
  '2025-01-15 12:35:00.123',
  'low',
  '00:00:05',
  '00:00:00.005',
  NULL,
  NULL,
  '00:00:04',
  '00:00:00.006'
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
