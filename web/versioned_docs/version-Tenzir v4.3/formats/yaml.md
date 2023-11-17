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
  allow-unsafe-pipelines: true
  endpoint: my-custom-endpoint:42
...
```

Convert the Tenzir configuration file to CSV:

```
from file ~/.config/tenzir/tenzir.yml | flatten | write csv
```

```tsv
tenzir.allow-unsafe-pipelines,tenzir.endpoint
true,my-custom-endpoint:42
```
