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

## Topics

**APIs and Patterns:**

- [data-access.md](./references/data-access.md) — Reading and iterating columnar data
- [data-building.md](./references/data-building.md) — Constructing series and table slices
- [variant-access.md](./references/variant-access.md) — Working with variants and match
- [error-handling.md](./references/error-handling.md) — TRY, check, and expected patterns
- [functions.md](./references/functions.md) — Implementing TQL functions
- [operators.md](./references/operators.md) — Implementing TQL operators (incl. secrets)

**Tooling and Conventions:**

- [external-files.md](./references/external-files.md) — Third-party code scaffold
- [utilities.md](./references/utilities.md) — Generic utilities in tenzir::detail
- [hashing.md](./references/hashing.md) — Hashing infrastructure
- [parser-combinators.md](./references/parser-combinators.md) — Parser combinator framework
- [common-types.md](./references/common-types.md) — Reusable common types
