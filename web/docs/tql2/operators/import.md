# import

Imports events into a Tenzir node. The dual to [`export`](export.md).

```
import
```

## Description

The `import` operator persists events in a Tenzir node.

## Examples

Import Zeek conn logs into a Tenzir node.
 
XXX: Fix example

```
from file conn.log read zeek-tsv | import
```
