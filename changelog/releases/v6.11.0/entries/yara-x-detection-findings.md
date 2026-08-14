---
title: YARA-X detection findings
type: change
authors:
  - mavam
created: 2026-08-13T19:05:25Z
---

The `yara` operator now uses YARA-X and emits one OCSF 1.9.0 Detection Finding for every matching rule by default. Findings include the applied rule, a readable and stable `yara:<namespace>:<identifier>` analytic identity, a unique finding identity, the scanned input's SHA-256 digest, and ordered match evidence with global count and encoded-size bounds. Set `format="plain"` to emit the original byte stream and native YARA rule metadata without constructing an OCSF finding.

This change removes the legacy positional syntax, `compiled_rules`, and the `yara.match` output. Specify exactly one rule source with `path=` or `rules=`:

```tql
from_file "suspicious.exe", mmap=true {
  yara path="/etc/tenzir/yara"
}
```

Use `rules=` for inline rules. New options control include directories, scan timeouts, maximum input size, and the maximum stored matches per pattern. Scans now time out after `1min` by default; set `timeout=` explicitly for workloads that need longer. The `vt` and `cuckoo` modules are unsupported because Tenzir does not provide their runtime data. YARA-X may also reject or interpret some legacy rules differently.
