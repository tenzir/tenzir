---
title: Package constants
type: feature
authors:
  - jachris
created: 2026-06-16T13:51:38.873991Z
---

A package can now define constant `let` bindings in a `constants.tql` file
at the package root. Each binding is evaluated to a constant when the
package loads and can be referenced as `pkg::$name` from the package's own
operators and pipelines, as well as from any external pipeline that uses
the package.

```tql
// acme/constants.tql
let $high_severity = 8
let $threshold = $high_severity + 1
```

```tql
// any pipeline, once the acme package is available
where severity >= acme::$threshold
```

Later bindings may reference earlier ones, as `$threshold` does above.
