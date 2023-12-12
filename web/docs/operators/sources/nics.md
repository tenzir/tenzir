# nics

Shows a snapshot of available network interfaces.

## Synopsis

```
nics
```

## Description

The `nics` operator shows a snapshot of all available network interfaces.

## Examples

List all connected network interfaces.

```
nics
| where status.connected == true'
```
