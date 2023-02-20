---
sidebar_position: 2
---

# Execute Sigma Rules

VAST can interpret [Sigma rules](https://github.com/SigmaHQ/sigma) as an
alternative to a [VAST query](/docs/understand/language). Simply
provide it on standard input to the `export` command:

```bash
vast export json < sigma-rule.yaml
```

This requires that you built VAST with the [Sigma
frontend](/docs/understand/language/frontends/sigma).

:::caution Compatibility
VAST does not yet support all Sigma features. Please consult the [compatbility
section](/docs/understand/language/frontends/sigma#compatibility) in
the documentation for details.
:::
