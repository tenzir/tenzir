---
name: developing-tenzir
description: Tenzir C++ idioms, APIs, and coding conventions. You MUST use this skill before writing any C++ or plans that contain C++. NEVER assume things based on surrounding code.
---

# Developing Tenzir

Tenzir-specific idioms, APIs, and abstractions for C++ development.

## Core Principle

Work on **columns**, not rows. Tenzir uses Apache Arrow for columnar data
processing. Evaluate expressions once per series or slice, then iterate if
row-wise access is needed.

## Topics

**APIs and Patterns:**

- [data-access.md](./references/data-access.md) — Reading and iterating columnar data
- [data-building.md](./references/data-building.md) — Constructing series and table slices
- [variant-access.md](./references/variant-access.md) — Working with variants and match
- [error-handling.md](./references/error-handling.md) — TRY, check, and expected patterns
- [functions.md](./references/functions.md) — Implementing TQL functions
- [operators.md](./references/operators.md) — Implementing TQL operators (incl. secrets)
- [executor.md](./references/executor.md) — Executor for operators and pipeline execution

**Tooling and Conventions:**

- [external-files.md](./references/external-files.md) — Third-party code scaffold
- [utilities.md](./references/utilities.md) — Generic utilities in tenzir::detail
- [hashing.md](./references/hashing.md) — Hashing infrastructure
- [parser-combinators.md](./references/parser-combinators.md) — Parser combinator framework
- [common-types.md](./references/common-types.md) — Reusable common types

**C++ Style:**

You MUST read these before writing any C++ or plans that contain C++. Do NOT assume things based on the surrounding code.

- [coding-conventions.md](./references/coding-conventions.md) — Formatting, style, and structure
- [naming-conventions.md](./references/naming-conventions.md) — Naming patterns and conventions
