# from_kafka 10M Perf Runner

This directory contains a reproducible manual benchmark for `from_kafka` using
our canonical perf fixture settings (`10M` seeded messages).

Run from repository root:

```bash
test/tests/perf/from_kafka_1m/run.sh
```

The runner:
- Starts the `kafka` fixture in foreground mode via `uvx tenzir-test --fixture`.
- Waits until the fixture exports `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
- Executes `tenzir --neo` with `TENZIR_KAFKA_FROM_PERF_STATS=1`.
- Prints wall time (`/usr/bin/time -p`) and `from_kafka` stage counters.
- Uses `test/tests/perf/from_kafka_1m/route53_sample.ndjson` by default.
- Seeding scale/trim is controlled by two fixture options:
  `messages` and `payload_file`.
  The fixture reuses payload lines as needed and emits exactly `messages`.

Common overrides:

```bash
PARTITIONS=1 MESSAGES=10000000 COMPRESSION=none test/tests/perf/from_kafka_1m/run.sh
PARTITIONS=4 MESSAGES=10000000 COMPRESSION=none test/tests/perf/from_kafka_1m/run.sh
PARTITIONS=1 MESSAGES=10000000 COMPRESSION=zstd test/tests/perf/from_kafka_1m/run.sh
PARTITIONS=4 MESSAGES=10000000 COMPRESSION=zstd test/tests/perf/from_kafka_1m/run.sh
PAYLOAD_FILE=test/tests/perf/from_kafka_1m/route53_sample.ndjson PARTITIONS=4 MESSAGES=10000000 test/tests/perf/from_kafka_1m/run.sh
TENZIR_BIN=./build/xcode/relwithdebinfo/bin/tenzir test/tests/perf/from_kafka_1m/run.sh
```

Notes:
- `test/tests/perf/test.yaml` is intentionally skipped in normal `tenzir-test`
  runs, so this script is the supported reproducible path for perf iteration.
