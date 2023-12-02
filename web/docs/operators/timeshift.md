---
sidebar_custom_props:
  operator:
    transformation: true
---

# timeshift

Adjust time values by anchoring them around the current time.

## Synopsis

```
timeshift [--speed <speed>] [--relative-to <relative-to>] <field>
```

## Description

The `timeshift` operators adjusts a series of time values by anchoring them
around the current time. Combined with the [`delay`](delay.md) operator this
enables replaying a historical data set in real-time.

### `--speed <speed>`

A constant factor that describes applied to the new time values. Set to 2.0 to
replay a dataset at twice the original speed, or to 0.5 to replay it at half the
original speed.

Defaults to 1.0.

### `--relative-to <relative-to>`

The time value to anchor the values around.

Defaults to the current time.

### `<field>`

The name fo the field for which to shift time values.

## Examples

Anchor the M57 Zeek data set around Dec 19th, 2023:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| replay --relative-to 2023-12-19 ts
```

Replay the M57 Zeek data set at ten times the original speed, shifting time
values to match the start time of the pipeline:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| replay --speed 10 ts
| delay ts
```
