**Authoritative reference:**

The integration-testing workflow lives in the standalone documentation at
<https://github.com/tenzir/test/blob/main/DOCUMENTATION.md>. Treat that file as
source of truth for project structure, configuration semantics, and runner
behaviour; revisit it whenever the CLI evolves.

**Quickstart recap:**

- Work from the `/test` project root (this repository) unless the task states
  otherwise.
- Run every command through the packaged CLI: `uvx tenzir-test …`.
- Discover available options with `uvx tenzir-test --help`.
- Pass paths under `tests/` (or use `--root` to target another project if the
  user asks).

**Common commands:**

```bash
uvx tenzir-test                        # run the entire suite
uvx tenzir-test tests/exec/foo.tql     # single test
uvx tenzir-test tests/exec/flows/      # directory
uvx tenzir-test --update tests/...     # refresh baselines
uvx tenzir-test -j 4                   # bound concurrency
```

Always capture the full CLI transcript for the user. When updating baselines,
validate that diffs match the intended behaviour before continuing.

**Project layout highlights:**

- `tests/` contains scenarios (TQL, Python, shell, …) with frontmatter that
  selects runners and fixtures.
- `inputs/` stores shared datasets; access them via the `TENZIR_INPUTS`
  environment variable inside tests.
- `fixtures/` and `runners/` expose Python hooks that the CLI imports on start.
- `test.yaml` files provide directory-scoped defaults; individual tests can
  override settings in their frontmatter.
- `tenzir.yaml` co-located with a test (or directory) supplies runtime
  configuration passed to the `tenzir` binary.

Refer to the documentation for detailed explanations of frontmatter keys,
runner registration, package mode, coverage collection, and advanced workflows.
