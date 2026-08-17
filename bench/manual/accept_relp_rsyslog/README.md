# rsyslog-to-Tenzir RELP benchmark

This harness sends a repeated 135-byte RFC 5424 message containing an OpenSSH
failed-password log through this path:

```text
Python sender -> rsyslog imtcp -> rsyslog omrelp -> Tenzir accept_relp
```

It stops after the requested number of events and reports the end-to-end rate.
The reported byte rate counts the generated Syslog input, including its newline,
but excludes TCP, TLS, and RELP framing overhead.

## Requirements

- Docker
- Python 3
- A Tenzir binary that provides `accept_relp`

## Run the benchmark

Set `TENZIR_BIN` when the binary is not on your `PATH`:

```sh
TENZIR_BIN=/path/to/tenzir ./run.sh 100000
```

The default `raw` variant measures RELP acceptance. Use `parsed` to include
Syslog and Grok parsing:

```sh
BENCHMARK_VARIANT=parsed TENZIR_BIN=/path/to/tenzir ./run.sh 100000
```

Set `BENCHMARK_DIAGNOSTICS=true` to enable debug output with RELP frames,
message batches, acknowledgements, transport writes, and queue stalls:

```sh
BENCHMARK_DIAGNOSTICS=true TENZIR_BIN=/path/to/tenzir ./run.sh 1000000
```

The connection statistics appear in `results/tenzir-stderr.txt`.

Run the command several times and report a median. This is a local integration
benchmark, not a capacity ceiling. Docker Desktop places rsyslog behind a Linux
VM, so every RELP acknowledgement crosses that boundary.
