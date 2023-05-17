# import

Imports events into a VAST node.

## Synopsis

```
import
```

## Description

The `import` operator persists events in a VAST node.

:::note Flush to disk
Pipelines ending in the `import` operator do not wait until all events in the
pipelines are written to disk.

We plan to change this behavior in the near future. Until then, we recommend
running `vast flush` after importing events to make sure they're available for
downstream consumption.
:::

## Examples

Import Zeek conn logs into a VAST node.

```
from file conn.log read zeek-tsv | import
```
