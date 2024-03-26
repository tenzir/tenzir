---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# bitz

Reads and writes BITZ, Tenzir's internal wire-format.

## Synopsis

```
bitz
```

## Description

The `bitz` format provides a parser and printer for Tenzir's internal
wire-format. It enables lossless transfer of events between Tenzir nodes, even
when a direct connection through a pipeline is not possible with Tenzir
directly.

BITZ is an unstable format, i.e., it cannot safely be written to disk and be
read again later with another Tenzir version.

:::info Did you know?
BITZ is short for **bi**nary **T**en**z**ir, and a play on the word bits.
:::

## Examples

Transfer events between two pipelines using `zmq`.

```text {0} title="Send bitz over zmq"
export
| to zmq://localhost:5670 write bitz
```

```text {0} title="Receive bitz from zmq"
from zmq://localhost:5670 read bitz
import
```
