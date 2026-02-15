# from_kafka 1M Perf Runner

This directory contains a reproducible manual benchmark for `from_kafka` using
our canonical perf fixture settings (`1M` seeded messages).

Run from repository root:

```bash
test/tests/perf/from_kafka_1m/run.sh
```

The runner:
- Starts the `kafka` fixture in foreground mode via `uvx tenzir-test --fixture`.
- Waits until the fixture exports `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
- Executes `tenzir --neo` with `TENZIR_KAFKA_FROM_PERF_STATS=1`.
- Prints wall time (`/usr/bin/time -p`) and `from_kafka` stage counters.

Common overrides:

```bash
BATCH_SIZE=50k FETCH_WAIT_TIMEOUT=16ms SINK=discard test/tests/perf/from_kafka_1m/run.sh
PARTITIONS=8 SEED_MESSAGES=1000000 test/tests/perf/from_kafka_1m/run.sh
TENZIR_BIN=./build/xcode/relwithdebinfo/bin/tenzir test/tests/perf/from_kafka_1m/run.sh
```

Notes:
- `test/tests/perf/test.yaml` is intentionally skipped in normal `tenzir-test`
  runs, so this script is the supported reproducible path for perf iteration.
