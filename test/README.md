# Tenzir Integration Tests

This directory contains the integration and scenario tests that exercise the
Tenzir binaries through the `tenzir-test` harness.

For comprehensive documentation, see:

- [Test Framework Reference](https://docs.tenzir.com/reference/test-framework)
- [Writing Tests Guide](https://docs.tenzir.com/guides/testing/write-tests)

## Getting Started

The tests require the Python-based `tenzir-test` harness and a Tenzir build
that exposes the `tenzir` and `tenzir-node` executables.

From the Tenzir repository root:

```sh
uvx tenzir-test --root test
```

From this directory:

```sh
uvx tenzir-test
```

The harness auto-detects inputs, fixtures, and runners relative to the current
working directory. If the `tenzir` binaries are not on `PATH`, specify them:

```sh
uvx tenzir-test \
  --tenzir-binary /path/to/tenzir \
  --tenzir-node-binary /path/to/tenzir-node
```

Run a single test or directory by passing its path:

```sh
uvx tenzir-test tests/exec/drop_null_fields
```

Use `-u` or `--update` to regenerate reference artifacts (`.txt`, `.diff` files, etc.)
after validating that the new output is correct.

## Configuration & Fixtures

Each test may define frontmatter to adjust execution:

```tql
---
runner: instantiation
fixtures: [node]
timeout: 60
---
```

Common defaults live in `test.yaml` files that apply recursively to their
subdirectories. For example, `tests/lexer/test.yaml` pins all lexer scenarios to
the `lexer` runner so individual files do not repeat the configuration.

Fixtures are registered in `fixtures/` and exposed via the `fixtures:` key.
Available fixtures include:

### Built-in Fixtures

- **`node`** – Spins up an ephemeral Tenzir node and exposes connection parameters
  through environment variables (used by node-centric scenarios).

### Project Fixtures

- **`http`** – HTTP echo server that records requests
  - `HTTP_FIXTURE_URL` – Server URL for client requests
  - `HTTP_FIXTURE_ENDPOINT` – Alias for `HTTP_FIXTURE_URL`
  - `HTTP_CAPTURE_FILE` – Path to file containing the last request/response

- **`tcp_tls_source`** – TLS-enabled TCP source that sends test data
  - `TCP_TLS_ENDPOINT` – TCP endpoint (e.g., `tcp://127.0.0.1:12345`)
  - `TCP_TLS_CERTFILE` – Path to server certificate
  - `TCP_TLS_KEYFILE` – Path to server private key
  - `TCP_TLS_CAFILE` – Path to CA certificate

- **`tcp_sink`** – TCP server that captures incoming connections
  - `TCP_SINK_ENDPOINT` – TCP endpoint to connect to
  - `TCP_SINK_FILE` – Path to file containing received data

- **`udp_source`** – UDP source that continuously sends test packets
  - `UDP_SOURCE_ENDPOINT` – UDP endpoint (e.g., `127.0.0.1:12345`)

- **`udp_sink`** – UDP server that captures incoming packets
  - `UDP_SINK_ENDPOINT` – UDP endpoint to send to
  - `UDP_SINK_FILE` – Path to file containing received data

When a test requests fixtures, the harness injects the appropriate environment
variables before invoking the runner.

**Example usage:**

```tql
---
fixtures: [udp_sink]
---
from {foo: 42}
to "udp://" + env("UDP_SINK_ENDPOINT")
```

The test can then verify the output by reading from the file at `UDP_SINK_FILE`.

## Available Runners

The harness always ships with the generic `tenzir`, `python`, and `custom`
runners. This repository registers additional diagnostic runners under
`test/runners/`:

| Runner          | Purpose                                                                             |
| --------------- | ----------------------------------------------------------------------------------- |
| `tenzir`        | Default runner for `.tql` pipelines, records stdout as the baseline artefact.       |
| `python`        | Executes Python scripts (`*.py`) and compares combined stdout/stderr with `.txt`.   |
| `custom`        | Runs shell fixtures (`*.sh`) via `sh -eu`, useful for orchestrating bespoke flows.  |
| `lexer`         | Captures the token stream (`tenzir --dump-tokens`) and stores a textual artefact.   |
| `ast`           | Dumps the abstract syntax tree (`tenzir --dump-ast`) for syntax-focused scenarios.  |
| `ir`            | Records the intermediate representation (`tenzir --dump-ir`).                       |
| `finalize`      | Shows the finalized pipeline (`tenzir --dump-finalized`).                           |
| `instantiation` | Produces a diff between `--dump-ir` and `--dump-inst-ir` to highlight rewrites.     |
| `opt`           | Diffs the instantiated IR against the optimized IR (`--dump-opt-ir`).               |
| `oldir`         | Exercises the legacy IR dumper (`--dump-pipeline`) for backwards-compatible suites. |

`test.yaml` files in the respective directories select the appropriate runner
automatically; override the runner in frontmatter when you need a different
view.

## Common Options

- `-u, --update` - Regenerate reference outputs after validating changes
- `-v, --verbose` - Enable verbose logging for detailed test information
- `-d, --debug` - Enable debug logging for framework diagnostics
- `-j, --jobs N` - Control test parallelism (default: number of CPUs)
- `-k, --keep` - Preserve per-test temporary directories for inspection
- `-p, --passthrough` - Stream raw test output directly to terminal
- `--coverage` - Enable code coverage collection (increases timeouts by 5x)
- `--purge` - Delete cached runner artifacts

## Updating & Debugging Tests

Refresh reference outputs after intentional behavior changes:

```sh
uvx tenzir-test -u tests/path/to/test
```

Control parallelism for debugging:

```sh
uvx tenzir-test -j 1 tests/path/to/test
```

Keep temporary directories for manual inspection:

```sh
uvx tenzir-test -k tests/path/to/test
```

If a test requires new fixtures or runners, add them under `fixtures/` or
`runners/` and import them in the corresponding `__init__.py` so the harness
registers them on startup.

For deeper debugging, you can run the generated command manually by inspecting
`tests/<path>.tql` and reproducing the runner invocation. The harness also
logs inherited configuration changes (via `test.yaml`) to help spot unintentional
overrides.
