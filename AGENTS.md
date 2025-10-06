# AI Agent Context for Tenzir

This document provides context and instructions for AI agents when working on
the Tenzir codebase.

## Project Overview

Tenzir is a low-code data pipeline solution that helps security teams to
collect, normalize, enrich, optimize, and route their data. The Tenzir Query
Language (TQL) is domain-specific language to write data pipelines.

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
  - Various Find\*.cmake modules for dependencies
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
  - `services/` - System service configurations
- `test/` - TQL integration test scenarios and expected outputs

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

### Run pipelines

Use the `tenzir` binary to execute a TQL program:

- The first argument is the pipeline definition.
- Alternatively, pass the pipeline defintion as file via `-f <path>`.
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

For more details, see the [Test Framework Reference](https://docs.tenzir.com/reference/test-framework) and [Writing Tests Guide](https://docs.tenzir.com/guides/testing/write-tests).

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

## Best Practices

- Follow the existing code style and conventions.
- Write integration tests for new TQL features, such as functions and
  operators.
- Use Git to create self-contained commits.
- Update documentation when adding new features.
- Changelog:
  - Create one or more a changelog entries for new features, bugfixes, or changes
    using `changelog/add.py` before creating a PR.
  - Write changelog entries like micro blog posts—always include examples to
    illustrate the feature or fix. For TQL examples:
    - Use ```tql blocks for both input pipelines and output
    - Show realistic, practical examples that demonstrate the feature
    - Include output that shows what users will actually see
  - Changelog titles should be user-focused: Write titles that describe the
    functionality from a user's perspective, not the technical implementation.

## Development Workflow

When contributing to the Tenzir code base, the development workflow typically
follows these high-level steps:

- Create a new topic branch: `git switch -c topic/feature-or-fix`
  - If you are already on a topic branch, ensure you are up-to-date with the
    latest changes and understand the changes up to the merge base
- Implement your changes
  - Use the **compiler** sub-agent to compile Tenzir
  - Use the **integration-tester** sub-agent to adapt integration tests
- Create a draft GitHub pull request via `gh`
- After the PR is open, write a changelog entry
  - Use the **changelog-writer** sub-agent to create the entry
  - Commit the changelog separately and push it to the topic branch
