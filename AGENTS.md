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
By default, discovery picks the directory with the most recently modified
`CMakeCache.txt`. Set `BUILD_DIR` to override this default.

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

## C++ development

Read the relevant references below before writing or planning any C++ code.
Do not assume patterns from surrounding code—older code may deviate from
current conventions.

### Core principle

Tenzir processes data in columns via Apache Arrow. Evaluate expressions per
series or slice first, then iterate only when row-wise access is necessary.

### Style

- [coding-conventions.md](.agents/references/coding-conventions.md): Formatting, structure, and idioms
- [naming-conventions.md](.agents/references/naming-conventions.md): Naming patterns

### APIs

- [data-access.md](.agents/references/data-access.md): Reading and iterating columnar data
- [data-building.md](.agents/references/data-building.md): Constructing series and table slices
- [data-conversion.md](.agents/references/data-conversion.md): Type-to-type conversion
- [variant-access.md](.agents/references/variant-access.md): Variants and match dispatch
- [error-handling.md](.agents/references/error-handling.md): TRY, check, and failure_or
- [functions.md](.agents/references/functions.md): TQL function plugins
- [operators.md](.agents/references/operators.md): TQL operator plugins and secrets
- [executor.md](.agents/references/executor.md): Executor and pipeline execution
- [operator-porting.md](.agents/references/operator-porting.md): Porting operators from `crtp_operator` to `Operator<Input, Output>`

### Rust-inspired vocabulary and helpers

Prefer these Tenzir abstractions over direct standard-library use in new code
when they model the same concept.

#### Vocabulary types

| Prefer      | Header                   | Instead of                   | Notes                                                     |
| ----------- | ------------------------ | ---------------------------- | --------------------------------------------------------- |
| `Arc<T>`    | `tenzir/arc.hpp`         | `std::shared_ptr<T>`         | Non-null, const-propagating shared ownership              |
| `Box<T>`    | `tenzir/box.hpp`         | `std::unique_ptr<T>`         | Non-null owning indirection with copy support             |
| `Option<T>` | `tenzir/option.hpp`      | `std::optional<T>`           | Optional value with reference support and monadic helpers |
| `Atomic<T>` | `tenzir/atomic.hpp`      | `std::atomic<T>`             | Copyable and movable atomic wrapper                       |
| `Ref<T>`    | `tenzir/ref.hpp`         | `std::reference_wrapper<T>`  | Non-owning reference with `->` and `*`                    |
| `Mutex<T>`  | `tenzir/async/mutex.hpp` | `std::mutex` + separate data | Async/fiber mutex guarding owned data                     |

#### Companion helpers

| Prefer           | Header              | Instead of               | Notes                                                              |
| ---------------- | ------------------- | ------------------------ | ------------------------------------------------------------------ |
| `None`           | `tenzir/option.hpp` | `std::nullopt`           | Empty `Option` tag                                                 |
| `panic(...)`     | `tenzir/panic.hpp`  | ad-hoc fatal checks      | Fails fast with formatted message, source location, and stacktrace |
| `TRY` / `CO_TRY` | `tenzir/try.hpp`    | manual error propagation | Rust `?`-style propagation for supported result types              |

### Tooling

- [external-files.md](.agents/references/external-files.md): Third-party code integration
- [utilities.md](.agents/references/utilities.md): Helpers in tenzir::detail
- [hashing.md](.agents/references/hashing.md): Hashing infrastructure
- [parser-combinators.md](.agents/references/parser-combinators.md): Parser combinator framework
- [common-types.md](.agents/references/common-types.md): Reusable types and abstractions
