# AI Agent Context for Tenzir

This document provides context and instructions for AI agents when working on
the Tenzir codebase.

## Project Overview

Tenzir is a low-code data pipeline solution that helps security teams to
collect, normalize, enrich, optimize, and route their data.

### Project Structure

- `.claude/` - Claude AI configuration
- `.github/` - GitHub configuration and CI/CD workflows
  - `workflows/` - CI pipelines for testing, building, releases, style checks
  - `dependabot.yml` - Dependency update automation
  - `labels.yml` - Issue and PR label definitions
- `changelog/` - Changelog management system
  - `changes/` - Individual change entries
  - `add.py` - Script to add new changelog entries
  - `release.py` - Script to generate release notes
- `cmake/` - CMake build system modules and utilities
  - Various Find*.cmake modules for dependencies
  - TenzirConfig.cmake.in - CMake configuration template
  - TenzirRegisterPlugin.cmake - Plugin registration utilities
- `docs/` - Documentation for functions and operators
  - `functions/` - Function reference documentation
  - `operators/` - Operator reference documentation
  - `openapi.node.yaml` - OpenAPI specification
- `libtenzir/` - Core Tenzir library
  - `include/tenzir/` - Public headers
  - `src/` - Implementation files
  - `builtins/` - Built-in operators, functions, formats, etc.
  - `aux/` - Vendored dependencies (CAF, simdjson, etc.)
  - `fbs/` - FlatBuffer schema definitions
  - `test/` - Unit tests
- `nix/` - Nix package management
  - `package.nix` - Main package definition
  - `overlay.nix` - Nix overlay for custom packages
  - Various dependency package definitions
- `plugins/` - Extension plugins
  - Each subdirectory is a plugin (amqp, kafka, s3, etc.)
  - Plugins contain CMakeLists.txt, source, and tests
- `scripts/` - Utility and maintenance scripts
  - Platform-specific installation scripts
  - Development tools and helpers
  - Analysis and debugging utilities
- `tenzir/` - Main executable and integration tests
  - `tenzir.cpp` - Main entry point
  - `bats/` - BATS integration test framework (deprecated)
  - `tests/` - TQL test files and expected outputs
  - `services/` - System service configurations

## Key Tasks

### Setup

Make sure submodules are initialized and updated:

```sh
git submodule update --init --recursive
```

### Build

Configure the build:

```sh
cmake -B build -D CMAKE_BUILD_TYPE=RelWithDebInfo
```

Use `CMAKE_BUILD_TYPE=Debug` only when in need of advanced debugging.

Build the project as follows:

```bash
cmake --build build
```

### Testing

Tenzir has both C++ *unit tests* and TQL *integration tests*.

#### Unit Tests

Run the unit tests via CTest:

```sh
ctest --test-dir build
```

#### Integration Tests

The integration tests are in `tenzir/tests`. Read `tenzir/tests/README.md` for
detailed instructions on how to exeucte and write tests.

TL;DR: run all integration tests via CMake:

```sh
cmake --build build --target integration
```

Or manually via `uv`:

```sh
uv run --python=>=3.12 tenzir/tests/run.py
```

### Code Quality

Tenzir maintains code quality through several mechanisms:

#### Code Formatting

- **C++ Code**: Uses clang-format with a custom `.clang-format` configuration
  - 2-space indentation
  - Attach braces style
  - Sorted includes with grouping
  - Run: `clang-format -i <file>` or use `scripts/clang-format-diff.py`

- **Python Code**: Uses Black formatter
  - Enforced in CI via `.github/workflows/style-check.yaml`
  - Configuration in `python/pyproject.toml`

#### CI/CD Pipeline

GitHub Actions workflows in `.github/workflows/`:

- `style-check.yaml` - Enforces code formatting standards
- `tenzir.yaml` - Main CI pipeline for builds and tests
- `analysis.yaml` - Static analysis and security checks
- `docker.yaml` - Container image builds
- `release.yaml` - Release automation

#### Linting Tools

- **shellcheck** for shell scripts (via `scripts/shellcheck.bash`)
- **clang-tidy** for C++ static analysis (when enabled)

#### Additional Tools

- `scripts/` directory contains various development utilities:
  - `git-setup.sh` - Git hooks and configuration
  - `regression-tests.sh` - Regression testing
  - Platform-specific dependency installers

## Development Guidelines

1. Follow the existing code style and conventions.
2. Write integration tests for new TQL features, such as functions and
   operators.
3. Update documentation when adding new features
