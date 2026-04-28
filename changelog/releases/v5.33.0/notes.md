This release makes the subnet function work directly with typed and string IP addresses, which removes boilerplate in TQL pipelines. It also fixes several stability issues in where, unroll, files, context::enrich, and collection indexing.

## 🚀 Features

### IP address support in subnet

The `subnet` function now accepts typed IP addresses, plain IP strings, and existing subnet values with an optional prefix length:

```tql
from {source_ip: 10.10.1.124}
net = subnet(source_ip, 24)
```

This returns `10.10.1.0/24` without converting the IP address to a string first. When you omit the prefix, IPv4 addresses become `/32` host subnets and IPv6 addresses become `/128` host subnets.

*By @mavam and @codex.*

## 🐞 Bug fixes

### Crash fix for deep left-associated where expressions

Tenzir no longer segfaults on some very deep left-associated boolean expressions in `where` clauses due to source-location handling.

*By @tobim and @codex in #6068.*

### Fixed unbounded memory growth `context::enrich`

We fixed an issue in the `context::enrich` operator that did cause unbounded memory growth.

*By @IyeOnline.*

### Large unroll output stability

The `unroll` operator no longer crashes when expanding very large lists into output that exceeds Arrow's per-array capacity.

*By @mavam and @codex.*

### Recursive files traversal of unreadable directories

The `files` operator now skips unreadable child directories during recursive traversal, emits a warning for each skipped directory by default, and continues listing accessible siblings. Set `skip_permission_denied=true` to ignore permission-denied paths silently: this suppresses warnings for skipped child directories and still makes an unreadable initial directory produce no events instead of an error. Non-permission filesystem errors continue to fail the pipeline.

*By @mavam and @codex.*

### Unsigned integer indexing in TQL

Both list and record indexing in TQL now work with signed and unsigned integer indices. This also applies to record field-position indexing and to the `get` function for records and lists.

*By @mavam and @codex.*
