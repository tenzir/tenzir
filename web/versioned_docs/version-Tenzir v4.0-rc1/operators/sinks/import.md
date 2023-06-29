# import

Imports events into a Tenzir node. The dual to [`export`](../sources/export.md).

## Synopsis

```
import
```

## Description

The `import` operator persists events in a Tenzir node.

:::note Flush to disk
Pipelines ending in the `import` operator do not wait until all events in the
pipelines are written to disk.

We plan to change this behavior in the near future. Until then, we recommend
running `tenzir-ctl flush` after importing events to make sure they're available
for downstream consumption.
:::

## Examples

Import Zeek conn logs into a Tenzir node.

```
from file conn.log read zeek-tsv | import
```
