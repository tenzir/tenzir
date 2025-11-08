# ADR 0001: User-defined operator parameter handling

- **Status**: Accepted — 2025-11-08
- **Glossary**: UDO = user-defined operator (packaged TQL operator)

## Context

Packages may ship TQL operators with YAML front matter that lists positional
arguments and named options. This ADR records the initial design for enabling
parameterized user-defined operators and, specifically, where argument
substitution should occur in the compilation pipeline.

## Decision

1. **Inline argument parsing in `operator_def::make`.**  
   Instead of delegating to `argument_parser2`, we parse positional and named
   arguments directly when instantiating a UDO. This logic enforces required
   parameters, applies defaults, validates selectors, and builds a substitution
   map based on the pre-parsed metadata, while keeping diagnostics tied to the
   invocation site.

2. **Use `ast::substitute_named_expressions` for parameter substitution.**  
   We added a reusable helper in `ast.cpp` that substitutes `$param` references
   while respecting `let` shadowing and selectors. UDO compilation now invokes
   this helper to rewrite the pipeline prior to compilation, removing the need
   for the bespoke `parameter_substituter`.

## Consequences

### Positive

- Diagnostics and future help output can rely on structured metadata without
  reparsing YAML strings.
- Parameter substitution is centralized and reuses existing AST tooling.
- Field-path and expression parameters share the same machinery, reducing edge
  cases.
- Users receive actionable feedback (usage, parameter table, suggestions) when
  invocation fails.

## Alternatives Considered

- **Dedicated preprocessing pass after parsing**  
  - *Upsides*: Produces a fully instantiated AST before later stages see it,
    potentially simplifying downstream passes and enabling caching of substituted
    pipelines per argument combination. Keeps instantiation orthogonal to the
    execution backend because substitution happens once at the start.  
  - *Downsides*: Requires an extra pipeline stage, duplicates metadata
    validation, and complicates diagnostics because errors would have to be
    mapped back to the original invocation. We ultimately rejected this option
    to avoid the additional bookkeeping across AST and IR compilation.

- **Continue using `argument_parser2`**  
  - *Upsides*: Reuses an existing, battle-tested component and keeps UDO
    invocation behaviour identical to native operators that still rely on
    `argument_parser2`. Deferring substitution until runtime could allow more
    dynamic expressions if we ever needed per-event argument evaluation.  
  - *Downsides*: Keeps parameter metadata opaque, making it hard to generate
    rich diagnostics or share logic with the IR backend. We also risked diverging
    behaviour between execution paths. We rejected this alternative in favour of
    a unified instantiation step driven by structured metadata.
