# Python Library

Tenzir ships with a Python library to enable interaction with Tenzir with
primitives that integrate well with the Python ecosystem. We distribute the
library as PyPI package called [tenzir][pypi-page].

[pypi-page]: https://pypi.org/project/tenzir/

:::warning Experimental
The Python library is considered experimental and subject to change without
notice.
:::

## Install the PyPI package

Use `pip` to install Tenzir:

```bash
pip install tenzir[module]
```

## Use the Tenzir Python library

### Quickstart

The following snippet illustrates a small script to query Tenzir.

```py
#!/usr/bin/env python3

import asyncio
from tenzir import Tenzir, to_json_rows

async def example():
    tenzir = Tenzir()

    generator = tenzir.export("192.168.1.103", limit=10)
    async for row in to_json_rows(generator):
        print(row)

asyncio.run(example())
```

### Overview

The Python library is meant to expose all the Tenzir features that are relevant
in a Python environment. For now though, it is still in active development and
only the following interfaces are exposed:

- `export`
- `count`
- `status`

Many options that exist on the CLI are not mapped to the library. The idea here
is to avoid overwhelming the API with options that are actually not needed when
interacting with Tenzir from Python.

### class Tenzir

```py
    class Tenzir(
        endpoint: Optional[str]
    )
```

Create a connection to a Tenzir node that is listening at the specified
endpoint. If no enpdoint is given the `TENZIR_ENDPOINT` environment variable is
used, if that is also not present the `tenzir.endpoint` value from a local
`tenzir.yaml` configuration file is used. In case that value is also not present
the default connection endpoint of `127.0.0.1:5158` is used.

#### export

```py
    coroutine export(
        expression: str,
        mode: ExportMode = ExportMode.HISTORICAL,
        limit: int = 100
    ) -> AsyncIterable[TableSlice]
```

Evaluate an expression in a Tenzir node and receive the resulting events in an
asynchronous stream of `TableSlices`.

The `mode` argument can be set to one of `HISTORICAL`, `CONTINUOUS`, or
`UNIFIED`. A historical export evaluates the expression against data
that is stored in the Tenzir database, the resulting output stream ends
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

Retrieve the current status from Tenzir.

```py
>>> st = await tenzir.status()
>>> pprint.pprint(st["system"])
{'current-memory-usage': 729628672,
 'database-path': '/var/lib/tenzir',
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

### class TenzirRow

A `TenzirRow` is a Python native representation of an "event" from Tenzir. It
consists of a `name` and a `data` dictionary.

```py
    coroutine to_json_rows(
        stream: AsyncIterable[TableSlice],
    ) -> AsyncIterable[TenzirRow]
```

Convert a stream of `TableSlice`s to a stream of `TenzirRow`s.
