---
title: Named path and rules arguments for the sigma operator
type: feature
authors:
  - mavam
created: 2026-08-13T06:31:58.802772Z
---

The `sigma` operator now accepts rule sources through the named `path=` and
`rules=` arguments.

Use `path=` with a file, a directory, or a list containing both:

```tql
sigma path=["rules/detection.yml", "rules/windows/"]
```

The operator searches directories recursively in deterministic order and loads
a file only once when overlapping paths refer to it. Explicitly named files no
longer need a `.yaml` or `.yml` extension. A rule file can also contain multiple
YAML documents, which the operator loads as separate rules.

Use `rules=` to embed YAML directly in a pipeline. The argument accepts one
string or a list of strings, and each string can contain a single rule or a
multi-document YAML collection:

```tql
sigma rules=r#"
title: Detect an administrator login
detection:
  selection:
    user: administrator
  condition: selection
"#
```

The operator validates embedded rules while constructing the pipeline and does
not access the filesystem for them. Empty and invalid embedded sources produce
a diagnostic at the `rules=` argument.

Passing a path positionally remains supported for compatibility, but now emits
a deprecation warning. Use `path=` instead. The `refresh_interval` argument
applies only to filesystem-backed sources and cannot be combined with `rules=`.
If a rule directory cannot be inspected during a refresh, the operator reports
the failure at the `path=` argument and keeps the previously loaded rules. A
rule source that no longer compiles also keeps its last valid version while
rules from other sources continue to update. The operator reports each broken
content revision once. Removing a file from a successfully inspected directory
removes its rules.
