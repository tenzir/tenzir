---
sidebar_position: 2
---

# Execute Sigma Rules

Tenzir can interpret [Sigma rules](https://github.com/SigmaHQ/sigma) as an
alternative to a [Tenzir query](../../understand/README.md). Simply
provide it on standard input to the `export` command:

```bash
tenzirctl export json < sigma-rule.yaml
```

This requires that you built Tenzir with the [Sigma
frontend](../../understand/frontends/sigma.md).

:::caution Compatibility
Tenzir does not yet support all Sigma features. Please consult the [compatbility
section](../../understand/frontends/sigma.md#compatibility) in the documentation
for details.
:::
