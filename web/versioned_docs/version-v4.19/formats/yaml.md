---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# yaml

Reads and writes YAML.

## Synopsis

```
yaml
```

## Description

The `yaml` format provides a parser and printer for YAML documents and streams.

## Examples

Print Tenzir's configuration as YAML:

```
show config | write yaml
```

```yaml
---
tenzir:
  no-location-overrides: true
  endpoint: my-custom-endpoint:42
...
```

Convert the Tenzir configuration file to CSV:

```
from file ~/.config/tenzir/tenzir.yml | flatten | write csv
```

```tsv
tenzir.no-location-overrides,tenzir.endpoint
true,my-custom-endpoint:42
```
