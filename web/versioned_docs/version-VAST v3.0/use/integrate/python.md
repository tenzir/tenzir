# Python

VAST ships with Python bindings to enable interaction with VAST with primitives
that integrate well with the Python ecosystem. We distribute the bindings as
PyPI package called [pyvast][pypi-page].

[pypi-page]: https://pypi.org/project/pyvast/

:::warning Experimental
PyVAST is considered experimental and subject to change without notice.
:::

## Install the PyPI package

Use `pip` to install PyVAST:

```bash
pip install pyvast
```

## Use PyVAST

### Quickstart

The following snippet illustrates a small script to query VAST.

```py
#!/usr/bin/env python3

import asyncio
from pyvast import VAST, to_json_rows

async def example():
    vast = VAST()

    generator = vast.export("192.168.1.103", limit=10)
    async for row in to_json_rows(generator):
        print(row)

asyncio.run(example())
```

### Overview

PyVAST is meant to expose all the VAST features that are relevant in a Python
environment. For now though, it is still in active development and only the
following interfaces are exposed:
- `export`
- `count`
- `status`

Many options that exist on the CLI are not mapped to PyVAST. The idea here is to
avoid overwhelming the API with options that are actually not needed when
interacting with VAST from Python.

### class VAST

```py
    class VAST(
        endpoint: Optional[str]
    )
```

Create a connection to a VAST node that is listening at the specified endpoint.
If no enpdoint is given the `VAST_ENDPOINT` environment variable is used, if
that is also not present the `vast.endpoint` value from a local `vast.yaml`
configuration file is used. In case that value is also not present the default
connection endpoint of `127.0.0.1:5158` is used.

#### export

```py
    coroutine export(
        expression: str,
        mode: ExportMode = ExportMode.HISTORICAL,
        limit: int = 100
    ) -> AsyncIterable[TableSlice]
```

Evaluate an expression in a VAST node and receive the resulting events in an
asynchronous stream of `TableSlices`.

The `mode` argument can be set to one of `HISTORICAL`, `CONTINUOUS`, or
`UNIFIED`. A historical export evaluates the expression against data
that is stored in the VAST database, the resulting output stream ends
when all eligible data has been evaluated. A `CONTINUOUS` one looks at data
as it flows into the node, it will continue to run until the event limit is
reached, it gets discarded, or the node terminates.

The `limit` argument sets an upper bound on the number of events that should
be produced. The special value `0` indicates that the number of results is
unbounded.

#### count

```py
    coroutine count(
        expression: str
    ) -> int
```

Evaluate the sum of all events in the database that match the given expression.

#### status

```py
    coroutine status() -> dict
```

Retrieve the current status from VAST.

```py
>>> st = await vast.status()
>>> pprint.pprint(st["system"])
{'current-memory-usage': 729628672,
 'database-path': '/var/lib/vast',
 'in-memory-table-slices': 0,
 'peak-memory-usage': 729628672,
 'swap-space-usage': 0}
```

### class TableSlice

```py
    coroutine collect_pyarrow(
        stream: AsyncIterable[TableSlice],
    ) -> dict[str, list[pyarrow.Table]]
```

Collect a stream of `TableSlice` and return a dictionary of [Arrow
tables][pyarrow] indexed by schema name.
[pyarrow]: https://arrow.apache.org/docs/python/index.html

### class VastRow

A `VastRow` is a Python native representation of an "event" from VAST. It
consists of a `name` and a `data` dictionary.

```py
    coroutine to_json_rows(
        stream: AsyncIterable[TableSlice],
    ) -> AsyncIterable[VastRow]
```

Convert a stream of `TableSlice`s to a stream of `VastRow`s.
