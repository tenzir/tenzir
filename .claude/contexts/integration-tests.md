**Authoritative reference:**

The integration-testing workflow is documented at:

- <https://docs.tenzir.com/reference/test-framework.md> - CLI reference and configuration
- <https://docs.tenzir.com/guides/testing/write-tests.md> - Guide for writing tests
- <https://docs.tenzir.com/guides/testing/create-fixtures.md> - Guide for creating fixtures
- <https://docs.tenzir.com/guides/testing/add-custom-runners.md> - Guide for adding runners

Treat these as the source of truth for project structure, configuration semantics,
and runner behaviour; revisit them whenever the CLI evolves.

**Quickstart recap:**

- The test suite lives in the `test/` subdirectory of the Tenzir repository.
- Work from either:
  - The Tenzir repository root (use `--root test` flag), OR
  - The `test/` subdirectory (no flag needed)
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
