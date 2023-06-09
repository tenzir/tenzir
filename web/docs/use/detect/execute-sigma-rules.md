---
sidebar_position: 2
---

# Execute Sigma Rules

Tenzir can interpret [Sigma rules](https://github.com/SigmaHQ/sigma) as an
alternative to a [Tenzir query](../../language.md). Simply
provide it on standard input to the `export` command:

```bash
tenzir-ctl export json < sigma-rule.yaml
```

This requires that you built Tenzir with the [Sigma
frontend](../../language/frontends/sigma.md).
