---
title: Node-wide parallelism
type: feature
authors:
  - aljazerzen
  - claude
created: 2026-08-19T09:56:48.784359Z
---

Parallelism is now configurable for an entire node with the
`tenzir.parallelism` option:

```yaml
tenzir:
  parallelism: max,limit_partitions=8
```

Every pipeline that does not request a degree itself runs with the configured
parallelism. Previously, the only ways to enable parallelism were the
`--parallelism` option of the `tenzir` binary, which does not apply to
pipelines running in a node, and a `// parallelism:` comment in a pipeline's
leading comment lines.

The option takes the same values as `--parallelism` and the `// parallelism:`
directive, and can also be set through the `TENZIR_PARALLELISM` environment
variable. A `// parallelism:` directive in a pipeline takes precedence over
`--parallelism`, which in turn takes precedence over `tenzir.parallelism`. Use
`// parallelism: disabled` to opt a single pipeline out of the configured
degree:

```tql
// parallelism: disabled
from_file "in.json"
where dest_port == 443
```

Invalid values now name their origin, so a typo in the configuration is no
longer mistaken for a typo in the pipeline:

```
error: invalid parallelism value in `tenzir.parallelism` configuration option
```
