# Tenzir

Tenzir is a data pipeline engine for security teams. It collects, parses,
shapes, enriches, and routes security telemetry with a unified dataflow
language. The Tenzir Query Language (TQL) is a domain-specific language for
building modular pipelines that process structured event data.

### Project structure

- `.agents/` - Agent-facing reference material
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

### Set up the checkout

Use checked-in entry points instead of invoking formatter binaries from `PATH`.
Tool versions are pinned in `lefthook.yml` or by the Nix formatter environment.

For non-Nix setups, install local hooks with:

```sh
npx --yes lefthook install
```

For Nix setups, install local hooks from the dev shell with:

```sh
lefthook install
```

After installing hooks, Git runs Lefthook's `pre-push` hook automatically. To
auto-fix local formatting issues, run Lefthook's `fix` hook on the files changed
by your branch. Pass files explicitly with `--file <path>` when running
Lefthook outside Git's actual pre-push flow.

Nix setups can also run the repository formatter with
`nix run .#format -- <path>...`. CI uses this Nix/treefmt path on PR-changed
files, so use it to reproduce and fix CI style failures.

### Configure the build

Configure a build by selecting a CMake prefix:

```sh
cmake --list-presets
cmake --preset <preset>
```

A build is configured when its directory contains `CMakeCache.txt`.

Before configuring a new build, run `scripts/build.sh --print-build-dir` to find
an existing configured build. Reuse it unless the task specifically requires a
different configuration.

### Compile the project

Compile a configured build:

```sh
scripts/build.sh
```

Pass a target only when needed:

```sh
scripts/build.sh tenzir-unit-test
```

### Run pipelines

Use the `tenzir` binary to execute a TQL program:

- The first argument is the pipeline definition.
- Alternatively, pass the pipeline definition as file via `-f <path>`.
- TQL files typically end with `.tql`.
- The pipeline may read stdin as data, based on the first operator.
- The pipeline may produce data on stdout, based on the last operator.

### Run integration tests

Build `tenzir` first when testing local changes:

```sh
scripts/build.sh
uvx tenzir-test --root test
```

Common `tenzir-test` options:

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

Read the relevant references below before writing, reviewing, or reasoning
about C++ code. Do not assume patterns from surrounding code—older code may
deviate from current conventions.

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
- [operator-review.md](.agents/references/operator-review.md): TQL operator review checklist

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
