# import

Imports events into a Tenzir node. The dual to [`export`](export.md).

```tql
import
```

## Description

The `import` operator persists events in a Tenzir node.

## Examples

Import Zeek conn logs into a Tenzir node.
 
```tql
load_file "conn.log" 
read_zeek_tsv
import
```
