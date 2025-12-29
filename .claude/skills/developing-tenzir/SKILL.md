---
name: developing-tenzir
description: Tenzir C++ idioms, APIs, and coding conventions. Use when implementing TQL functions, operators, working with series/table_slice, Arrow arrays, view3 abstractions, or asking about Tenzir code style.
---

# Developing Tenzir

Tenzir-specific idioms, APIs, and abstractions for C++ development.

## Core Principle

Work on **columns**, not rows. Tenzir uses Apache Arrow for columnar data
processing. Evaluate expressions once per series or slice, then iterate if
row-wise access is needed.

## Quick Reference

| Task                  | Use                    | Avoid                    |
| --------------------- | ---------------------- | ------------------------ |
| Iterate series        | `values3(array)`       | `values()`, `value_at()` |
| Check variant type    | `is<T>(x)`             | `std::holds_alternative` |
| Get variant value     | `as<T>(x)`             | `std::get`               |
| Safe variant access   | `try_as<T>(x)`         | `std::get_if`            |
| Multi-case dispatch   | `match(x, ...)`        | switch/if chains         |
| Early return on error | `TRY(expr)`            | manual if/return         |
| Assert with location  | `check(expr)`          | `TENZIR_ASSERT`          |
| Build Arrow arrays    | `check(b.Append(...))` | unchecked calls          |

## Topics

Read specific files only when you need that topic:

**APIs and Patterns:**

- [data-access.md](./references/data-access.md) — Reading and iterating columnar data
- [data-building.md](./references/data-building.md) — Constructing series and table slices
- [variant-access.md](./references/variant-access.md) — Working with variants and match
- [error-handling.md](./references/error-handling.md) — TRY, check, and expected patterns
- [functions.md](./references/functions.md) — Implementing TQL functions
- [operators.md](./references/operators.md) — Implementing TQL operators
- [secrets.md](./references/secrets.md) — Resolving secrets in operators

**Tooling and Conventions:**

- [clang-format.md](./references/clang-format.md) — Formatting settings
- [clang-tidy.md](./references/clang-tidy.md) — Linting settings
- [unit-tests.md](./references/unit-tests.md) — Test structure and fixtures
- [external-files.md](./references/external-files.md) — Third-party code scaffold
