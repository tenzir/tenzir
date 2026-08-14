---
title: Sigma global filter rules
type: feature
authors:
  - mavam
created: 2026-08-13T11:12:02.914739Z
---

The `sigma` operator now supports Sigma v2.1 global filters, which let you tune
multiple detection rules without modifying the original rules.

For example, the following multi-document YAML file detects a suspicious
process but suppresses matches from known administrator accounts:

```yaml
title: Suspicious process
id: 6f3e2987-db24-4c78-a860-b4f4095a7095
logsource:
  category: process_creation
detection:
  selection:
    Image|endswith: '\evil.exe'
  condition: selection
---
title: Ignore administrator accounts
logsource:
  category: process_creation
filter:
  rules:
    - 6f3e2987-db24-4c78-a860-b4f4095a7095
  administrator:
    User|startswith: adm_
  condition: not administrator
```

A filter can target rules by `id` or `name`. Use `rules: any` instead of a list
to apply a filter to every rule with a compatible log source. You can keep
filters next to their target rules in multi-document YAML, place them in
separate files, or pass them through the inline `rules=` option.

If a filter references an unknown or ambiguous rule, or its log source is
incompatible with a target, the operator emits a warning and continues running
the affected rules without that filter. When a filter becomes invalid during a
hot reload, the operator keeps its last valid version active until you provide
a working replacement.
