# Tenzir Integration Tests

This directory contains the integration and scenario tests that exercise the
Tenzir binaries through the `tenzir-test` harness. The notes below explain how
to run the suite, how to work with fixtures and runners, and how to update
expectations when behaviour changes.

## Getting Started

The tests require the Python-based `tenzir-test` harness (shipped from the
`tenzir-test` repository) and a Tenzir build that exposes the `tenzir` and
`tenzir-node` executables. From the repository root you can execute:

```sh
uvx tenzir-test
```

The harness auto-detects inputs, fixtures, and runners relative to the current
working directory. If the `tenzir` binaries are not on `PATH`, point the harness
at your build with:

```sh
TENZIR_BINARY=/path/to/tenzir \
TENZIR_NODE_BINARY=/path/to/tenzir-node \
uvx tenzir-test
```

Run a single test or directory by passing its path instead of the root:

```sh
uvx tenzir-test tests/exec/drop_null_fields
```

Use `-u/--update` to rewrite reference artefacts (for example `.txt` or `.diff`
files) after validating that the new output is correct.

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
Useful helpers include:

- `node` – spins up an ephemeral Tenzir node and exposes connection parameters
  through the environment (used by node-centric scenarios).
- `http` or project-specific helpers as defined in `fixtures/__init__.py`.

When a test requests fixtures, the harness injects the appropriate environment
variables before invoking the runner.

## Available Runners

The harness always ships with the generic `tenzir`, `python`, and `custom`
runners. This repository registers additional diagnostic runners under
`test/runners/`:

| Runner          | Purpose                                                                              |
| --------------- | ------------------------------------------------------------------------------------ |
| `tenzir`        | Default runner for `.tql` pipelines, records stdout as the baseline artefact.        |
| `python`        | Executes Python scripts (`*.py`) and compares combined stdout/stderr with `.txt`.    |
| `custom`        | Runs shell fixtures (`*.sh`) via `sh -eu`, useful for orchestrating bespoke flows.   |
| `lexer`         | Captures the token stream (`tenzir --dump-tokens`) and stores a textual artefact.    |
| `ast`           | Dumps the abstract syntax tree (`tenzir --dump-ast`) for syntax-focused scenarios.   |
| `ir`            | Records the intermediate representation (`tenzir --dump-ir`).                        |
| `finalize`      | Shows the finalized pipeline (`tenzir --dump-finalized`).                            |
| `instantiation` | Produces a diff between `--dump-ir` and `--dump-inst-ir` to highlight rewrites.      |
| `opt`           | Diffs the instantiated IR against the optimized IR (`--dump-opt-ir`).                |
| `oldir`         | Exercises the legacy IR dumper (`--dump-pipeline`) for backwards-compatible suites. |

`test.yaml` files in the respective directories select the appropriate runner
automatically; override the runner in frontmatter when you need a different
view.

## Updating & Debugging Tests

- Run `uvx tenzir-test -u path/to/test` to refresh reference
  outputs after intentional behaviour changes.
- Set `TENZIR_TEST_LOG_COMPARISONS=1` to print file comparisons when diagnosing
  mismatches.
- Combine `--log-comparisons` and `--purge` to get detailed diffs and remove
  stale artefacts.
- If a test requires new fixtures or runners, add them under `fixtures/` or
  `runners/` and import them in the corresponding `__init__.py` so the harness
  registers them on startup.

For deeper debugging, you can run the generated command manually by inspecting
`tests/<path>.tql` and reproducing the runner invocation. The harness also
logs inherited configuration changes (via `test.yaml`) to help spot unintentional
overrides.
