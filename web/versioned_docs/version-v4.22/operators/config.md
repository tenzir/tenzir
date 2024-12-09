---
sidebar_custom_props:
  operator:
    source: true
---

# config

Shows the node's configuration.

## Synopsis

```
config
```

## Description

The `config` operator shows the node's configuration, merged from the various
configuration files, command-line options, and environment variables.

## Examples

Write the entire configuration file as YAML:

```
config
| write yaml
```
