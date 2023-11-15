# import

Imports events into a Tenzir node. The dual to [`export`](../sources/export.md).

## Synopsis

```
import
```

## Description

The `import` operator persists events in a Tenzir node.

## Examples

Import Zeek conn logs into a Tenzir node.

```
from file conn.log read zeek-tsv | import
```
