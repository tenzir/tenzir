---
title: Secret type support for user-defined operator parameters
type: bugfix
authors:
  - mavam
  - claude
pr: 5752
created: 2026-02-12T17:26:03.709677Z
---

User-defined operators in packages can now declare parameters with the `secret` type to ensure that secret values are properly handled as secret expressions:

```
args:
  positional:
    - name: api_key
      type: secret
      description: "API key to use for authentication"
```
