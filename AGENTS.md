# Tenzir

Tenzir is a data pipeline engine for security teams. It collects, parses,
shapes, enriches, and routes security telemetry with a unified dataflow
language. The Tenzir Query Language (TQL) is a domain-specific language for
building modular pipelines that process structured event data.

### Project Structure

- `.claude/` - Claude Code configuration
  - `agents/` - Custom agent definitions
  - `commands/` - Custom slash commands
  - `skills/` - Custom skills and their references
  - `settings.json` - Project settings and enabled plugins
- `.docs/` - Documentation website (Astro-based)
- `.github/` - GitHub configuration and CI/CD workflows
  - `workflows/` - CI pipelines for testing, building, releases, style checks
  - `dependabot.yml` - Dependency update automation
  - `labeler.yml` - PR auto-labeling configuration
- `changelog/` - Changelog entries and release configuration
- `cmake/` - CMake build system modules and utilities
  - Various Find\*.cmake modules for dependencies
  - TenzirConfig.cmake.in - CMake configuration template
  - TenzirRegisterPlugin.cmake - Plugin registration utilities
- `libtenzir/` - Core Tenzir library
  - `include/tenzir/` - Public headers
  - `src/` - Implementation files
  - `builtins/` - Built-in TQL operators, functions, formats, connectors, etc.
  - `aux/` - Vendored dependencies (CAF, simdjson, etc.)
  - `fbs/` - FlatBuffer schema definitions
  - `test/` - Unit tests
- `libtenzir_test/` - Test utilities library
- `nix/` - Nix package management
  - `package.nix` - Main package definition
  - `overlay.nix` - Nix overlay for custom packages
- `plugins/` - Extension plugins
  - Each subdirectory is a plugin (amqp, kafka, s3, etc.)
  - Plugins contain CMakeLists.txt, source, and tests
- `python/` - Python packages and tooling
  - `tenzir/` - Core Python package
  - `tenzir-common/` - Common utilities
  - `tenzir-operator/` - Kubernetes operator
- `scripts/` - Utility and maintenance scripts
  - Platform-specific installation scripts
  - Development tools and helpers
  - Analysis and debugging utilities
- `tenzir/` - Main executable
  - `tenzir.cpp` - Main entry point
  - `services/` - System service configurations
- `tenzir-unit-test/` - Unit test executable
- `test/` - TQL integration test scenarios and expected outputs

## Key Tasks

### Setup

Make sure submodules are initialized and updated:

```sh
git submodule update --init --recursive
```

### Build

Assume the user has configured the build project, which they typically do by:

```sh
cmake --preset "<preset>"
```

When the build is configured, compile via `/compile`.

### Run pipelines

Use the `tenzir` binary to execute a TQL program:

- The first argument is the pipeline definition.
- Alternatively, pass the pipeline definition as file via `-f <path>`.
- TQL files typically end with `.tql`.
- The pipeline may read stdin as data, based on the first operator.
- The pipeline may produce data on stdout, based on the last operator.

### Integration Tests

Run integration tests from the repository root:

```sh
uvx tenzir-test --root test
```

Or from within the `test/` directory:

```sh
cd test
uvx tenzir-test
```

Common options:

- `--update` or `-u`: Update reference outputs after validating changes
- `--jobs N` or `-j N`: Control parallelism (default: number of CPUs)
- `--debug` or `-d`: Show detailed test information

For more details, read the [Test Framework reference
docs](https://docs.tenzir.com/reference/test-framework.md) and [guide on writing
tests](https://docs.tenzir.com/guides/testing/write-tests.md).
