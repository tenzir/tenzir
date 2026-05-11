# accept_udp 1M Perf Runner

This directory contains a reproducible manual benchmark for `accept_udp` using
the UDP fixture by default.

Run from the repository root:

```bash
test/tests/perf/accept_udp_1m/run.sh
```

The runner:

- Starts the `udp` fixture via `uvx tenzir-test --fixture` in client mode.
- Waits until the fixture exports `UDP_ENDPOINT`.
- Executes `tenzir` with `accept_udp`, stops after `COUNT` events, and
  discards the result by default.
- Prints wall time via `/usr/bin/time -p`.

Common overrides:

```bash
COUNT=10000000 test/tests/perf/accept_udp_1m/run.sh
PAYLOAD='hello' COUNT=1000000 test/tests/perf/accept_udp_1m/run.sh
PAYLOAD_HEX=ff BINARY=true COUNT=1000000 test/tests/perf/accept_udp_1m/run.sh
TENZIR_BIN=./build/xcode/release/bin/tenzir test/tests/perf/accept_udp_1m/run.sh
```

For local experiments with a native UDP generator, set `GENERATOR=udpgen` and
point `UDPGEN_BIN` at a compatible `udpgen` binary:

```bash
GENERATOR=udpgen \
UDPGEN_BIN=/tmp/udpgen_syslog \
UDPGEN_RATE=0 \
UDPGEN_THREADS=1 \
UDPGEN_PACKETS_PER_SEND=100 \
COUNT=500000 \
UDPGEN_COUNT=2000000 \
test/tests/perf/accept_udp_1m/run.sh
```

Notes:

- `INTERVAL=0.0` makes the fixture send datagrams as fast as its Python worker
  can produce them. Increase it to throttle the sender.
- `INITIAL_DELAY=1.0` gives the script time to start `accept_udp` before the
  fixture begins sending datagrams.
