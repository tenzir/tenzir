"""DuckDB fixture for the `from_duckdb` and `to_duckdb` operators.

Creates a temporary directory for database files and seeds one database
through the `duckdb` CLI, so that reads exercise types and storage that
Tenzir did not write itself.

Environment variables yielded:
- DUCKDB_ROOT: Absolute path to the temporary directory.
- DUCKDB_SEED: Path to the seeded database file `seed.duckdb`.
- DUCKDB_CLI: Path to the `duckdb` CLI, for Python verification steps.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable

logger = logging.getLogger(__name__)

SEED_SQL = r"""
CREATE TABLE events (
    id INTEGER,
    ts TIMESTAMP,
    ts_ns TIMESTAMP_NS,
    day DATE,
    name VARCHAR,
    score DOUBLE,
    ratio FLOAT,
    ok BOOLEAN,
    small SMALLINT,
    big UBIGINT,
    amount DECIMAL(10, 2),
    tags VARCHAR[],
    meta STRUCT(host VARCHAR, port INTEGER),
    kv MAP(VARCHAR, INTEGER),
    pair INTEGER[2],
    payload BLOB,
    took INTERVAL
);
INSERT INTO events VALUES
    (1, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123456789', '2024-01-01',
     'alpha', 1.5, 0.25, true, 7, 18446744073709551615, 12.34,
     ['a', 'b'], {host: 'h1', port: 80}, MAP {'x': 1}, [1, 2], '\x01\x02'::BLOB,
     INTERVAL 90 SECOND),
    (2, '2024-01-02 11:30:00', NULL, NULL, NULL, NULL, NULL, false, NULL,
     0, NULL, [], {host: NULL, port: 443}, MAP {}, NULL, NULL, NULL),
    (3, NULL, '2024-01-03 00:00:00', '2024-01-03', 'gamma', -2.0, 1.0, NULL,
     -7, 42, 0.01, NULL, NULL, MAP {'y': 2, 'z': 3}, [3, NULL], '',
     INTERVAL 1 DAY);
CREATE SCHEMA staging;
CREATE TABLE staging.numbers AS
    SELECT range AS n, range * 2 AS doubled FROM range(5000);
CREATE TABLE keyed (id INTEGER PRIMARY KEY, n INTEGER, name VARCHAR);
INSERT INTO keyed VALUES (1, 100, 'one'), (2, 200, 'two'), (3, 300, 'three');
CREATE TABLE huge_keyed (id HUGEINT PRIMARY KEY, name VARCHAR);
INSERT INTO huge_keyed VALUES
    (170141183460469231731687303715884105726, 'big'),
    (170141183460469231731687303715884105727, 'bigger');
CREATE TABLE bignum_keyed (id BIGNUM, name VARCHAR);
INSERT INTO bignum_keyed VALUES
    ('123456789012345678901234567890123456789012345678901', 'big'),
    ('123456789012345678901234567890123456789012345678902', 'bigger');
CREATE TABLE names (name VARCHAR);
INSERT INTO names VALUES ('a'), ('b');
"""


@fixture()
def duckdb() -> FixtureHandle:
    cli = shutil.which("duckdb")
    if cli is None:
        raise FixtureUnavailable(
            "The duckdb fixture requires the `duckdb` CLI on the PATH."
        )
    # Resolve symlinks so that paths in diagnostics are canonical. On macOS,
    # /tmp is a symlink to /private/tmp.
    root = Path(tempfile.mkdtemp(prefix="tenzir-test-duckdb-")).resolve()
    seed = root / "seed.duckdb"
    try:
        subprocess.run(
            [cli, str(seed)],
            input=SEED_SQL,
            text=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(root, ignore_errors=True)
        raise RuntimeError(f"failed to seed DuckDB database: {exc.stderr}") from exc
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise
    logger.info("Seeded DuckDB database at %s", seed)

    def _teardown() -> None:
        shutil.rmtree(root, ignore_errors=True)
        logger.info("Cleaned up temp directory: %s", root)

    return FixtureHandle(
        env={
            "DUCKDB_ROOT": str(root),
            "DUCKDB_SEED": str(seed),
            "DUCKDB_CLI": cli,
        },
        teardown=_teardown,
    )
