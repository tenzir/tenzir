---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# yaml

Reads and writes YAML.

## Synopsis

Parser:
```
yaml [--merge] [--schema <schema>] [--selector <fieldname[:prefix]>]
     [--schema-only] [--raw] [--unnest-separator <separator>]
```
Printer:
```
yaml
```

## Description

The `yaml` format provides a parser and printer for YAML documents and streams.

### Common Options (Parser)

The options are the common parser options, which can be found on the [formats page](formats.md#parser-schema-inference).

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
