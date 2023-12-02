---
sidebar_custom_props:
  operator:
    transformation: true
---

# delay

Delay events relative to a given start time with an optional speedup.

## Synopsis

```
delay [--start <time>] [--speed <factor>] <field>
```

## Description

Delay events relative to a given start time with an optional speedup.
FIXME

### `--start <time>`

The time value to anchor the values around.

Defaults to the first non-null timestamp.

### `--speed <factor>`

FIXME

## Examples

Replay the M57 Zeek data set.

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| delay ts
```

Replay the M57 Zeek data set at ten times the original speed:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| delay --speed 10 ts
```

FIXME: Good example for timeshift+delay

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| timeshift --start "2 days ago"
| delay --start "1 day ago" --speed 10 ts
```
