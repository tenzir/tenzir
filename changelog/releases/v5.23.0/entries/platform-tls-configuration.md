---
title: Platform TLS configuration
type: feature
authors:
  - lava
pr: 5559
created: 2025-12-17T16:33:15.093565Z
---

The Tenzir Node now lets you configure the minimum TLS version and TLS ciphers
accepted for the connection to the Tenzir Platform:

```yaml
plugins:
  platform:
    tls-min-version: "1.2"
    tls-ciphers: "HIGH:!aNULL:!MD5"
```
