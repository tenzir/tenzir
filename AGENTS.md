# Tenzir

Tenzir is a data pipeline engine for security teams. It collects, parses,
shapes, enriches, and routes security telemetry with a unified dataflow
language. The Tenzir Query Language (TQL) is a domain-specific language for
building modular pipelines that process structured event data.

### Project structure

- `.agents/` - Agent-specific configuration and reference material
- `.claude/` - Claude Code configuration
- `.docs/` - Optional local clone of the `tenzir/docs` repository
- `.github/` - GitHub configuration and CI/CD workflows
- `changelog/` - Changelog entries and release metadata
  - Managed with the `tenzir-ship` framework
  - Reference: https://docs.tenzir.com/reference/ship-framework.md
- `cmake/` - CMake build system modules and utilities
  - Various CMake modules for dependencies
  - TenzirConfig.cmake.in - CMake configuration template
  - TenzirRegisterPlugin.cmake - Plugin registration utilities
- `libtenzir/` - Core Tenzir library
  - `include/tenzir/` - Public headers
  - `src/` - Implementation files
  - `builtins/` - Built-in operators, functions, formats, connectors, and related plugins
  - `aux/` - Vendored dependencies (CAF, simdjson, etc.)
  - `fbs/` - FlatBuffer schema definitions
  - `test/` - Unit tests
- `libtenzir_test/` - Test utilities library
- `nix/` - Nix package management
  - `package.nix` - Main package definition
  - `overlay.nix` - Nix overlay for custom packages
- `plugins/` - Additional plugins outside `libtenzir/builtins`
  - Each subdirectory is a plugin (amqp, kafka, s3, etc.)
- `python/` - Python packages and tooling
- `scripts/` - Utility and maintenance scripts
  - Platform-specific installation scripts
  - Development tools and helpers
  - Analysis and debugging utilities
- `tenzir/` - Main executable
  - `tenzir.cpp` - Main entry point
  - `services/` - System service configurations
- `test/` - Integration tests and expected outputs

## Key tasks

### Configure the build

A build is configured when its directory contains `CMakeCache.txt`. A build
directory may 3 levels down from the project root.

Only when explicitly asked to configure a build, run:

```sh
cmake --list-presets
cmake --preset <preset>
```

### Compile the project

Run `/compile` to compile the build. Fall back to `scripts/build.sh` if
`/compile` is unavailable.

These commands auto-discover a configured build directory under `build/`.

### Run pipelines

Use the `tenzir` binary to execute a TQL program:

- The first argument is the pipeline definition.
- Alternatively, pass the pipeline definition as file via `-f <path>`.
- TQL files typically end with `.tql`.
- The pipeline may read stdin as data, based on the first operator.
- The pipeline may produce data on stdout, based on the last operator.

### Run integration tests

> Prerequisite: ensure your build's `bin/` directory is in `$PATH`.

Run integration tests from the repository root:

```sh
uvx tenzir-test --root test
```

Common options:

- `--passthrough`: Stream output to the terminal (skips reference comparison)
- `--update`: Update reference outputs (check correctness before or after)
- `--debug`: Show detailed test information
- `--match`: Filter test paths with substring or glob matching

Reference documentation: https://docs.tenzir.com/reference/test-framework.md

### Run unit tests

Run unit tests using the `tenzir-unit-test` binary:

```sh
tenzir-unit-test
```

### Update documentation

User-facing documentation lives in the git-ignored `.docs/` directory, which is
an optional clone of the `tenzir/docs` repository.

When changing existing behavior or adding user-facing functionality, update
`.docs/`, create a topic branch there, and open a companion PR against
`tenzir/docs`.

Skip this process for internal refactorings that do not affect the user-facing
TQL surface or command line tools.

## Developing Tenzir

Tenzir-specific idioms, APIs, and abstractions for C++ development. You MUST
read the relevant references before writing any C++ code. NEVER assume patterns
based on surrounding code alone.

### Core Principle

Work on **columns**, not rows. Tenzir uses Apache Arrow for columnar data
processing. Evaluate expressions once per series or slice, then iterate if
row-wise access is needed.

### C++ Style

You MUST read these before writing any C++ or plans that contain C++:

- [coding-conventions.md](.agents/references/coding-conventions.md): Formatting, style, and structure
- [naming-conventions.md](.agents/references/naming-conventions.md): Naming patterns and conventions

### APIs and Patterns

- [data-access.md](.agents/references/data-access.md): Reading and iterating columnar data
- [data-building.md](.agents/references/data-building.md): Constructing series and table slices
- [data-conversion.md](.agents/references/data-conversion.md): Converting between types
- [variant-access.md](.agents/references/variant-access.md): Working with variants and match
- [error-handling.md](.agents/references/error-handling.md): TRY, check, and expected patterns
- [functions.md](.agents/references/functions.md): Implementing TQL functions
- [operators.md](.agents/references/operators.md): Implementing TQL operators (incl. secrets)
- [executor.md](.agents/references/executor.md): Executor for operators and pipeline execution

### Tooling and Conventions

- [external-files.md](.agents/references/external-files.md): Third-party code scaffold
- [utilities.md](.agents/references/utilities.md): Generic utilities in tenzir::detail
- [hashing.md](.agents/references/hashing.md): Hashing infrastructure
- [parser-combinators.md](.agents/references/parser-combinators.md): Parser combinator framework
- [common-types.md](.agents/references/common-types.md): Reusable common types
