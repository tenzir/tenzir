# Run benchmarks locally

Run a selected benchmark from the `engine` directory. You need Docker for
benchmarks that use service fixtures, such as `from_kafka_route53`.

1. Check out the benchmark runner and create an isolated environment:

   ```sh
   mkdir -p .tools
   gh repo clone tenzir/bench .tools/tenzir-bench
   nix develop -c python3 -m venv .venv/bench
   nix develop -c .venv/bench/bin/pip install .tools/tenzir-bench
   ```

2. Run a benchmark against a Docker image, a static artifact, or a `tenzir`
   binary. This command runs only the Kafka benchmark and does not publish a
   reference report:

   ```sh
   nix develop -c .venv/bench/bin/python scripts/bench-ci/run_benchmarks.py \
     reference --bench-root bench \
     --target ghcr.io/tenzir/tenzir:v6.9.0 \
     --benchmark from_kafka_route53
   ```

Use `--destination s3://BUCKET/PREFIX` only when you intend to publish the
result as a reference.

## Run the manual RELP benchmark

The `manual/accept_relp_rsyslog` harness measures acknowledged rsyslog delivery
through `accept_relp`, with optional Syslog and Grok parsing. It targets a local
Tenzir binary and keeps its Docker fixture, sender, and recorded results
together:

```sh
TENZIR_BIN="$(scripts/build.sh --print-build-dir)/bin/tenzir" \
  bench/manual/accept_relp_rsyslog/run.sh 100000
```
