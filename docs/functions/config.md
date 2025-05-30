---
title: config
---

Reads Tenzir's configuration file.

```tql
config() -> record
```

## Description

The `config` function retrieves Tenzir's configuration, including values from
various `tenzir.yaml` files, plugin-specific configuration files, environment
variables, and command-line options.

Note that the `tenzir.secrets`, `tenzir.token` and `caf` options are omitted
from the returned record. The former to avoid leaking secrets, the latter as it
only contains internal performance-related that are developer-facing and should
not be relied upon within TQL.

## Examples

### Provide a name mapping in the config file

```yaml title="/opt/tenzir/etc/tenzir/tenzir.yaml"
flags:
  de: 🇩🇪
  us: 🇺🇸
```

```tql
let $flags = config().flags
from (
  {country: "de"},
  {country: "us"},
  {country: "uk"},
)
select flag = $flags.get(country, "unknown")
```

```tql
{flag: "🇩🇪"}
{flag: "🇺🇸"}
{flag: "unknown"}
```

## See also

[`env`](/reference/functions/env),
[`secret`](/reference/functions/secret)
