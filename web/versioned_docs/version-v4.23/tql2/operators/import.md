# import

Imports events into a Tenzir node. The dual to [`export`](export.md).

```tql
import
```

## Description

The `import` operator persists events in a Tenzir node.

## Examples

### Import Zeek connection logs in TSV format

```tql
load_file "conn.log"
read_zeek_tsv
import
```
