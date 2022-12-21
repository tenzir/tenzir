---
description: Columnar format for analytics workloads
---

# Arrow

VAST supports reading and writing data in the binary [`Arrow IPC`][arrow-ipc]
columnar format, suitable for efficient handling of large data sets. For
example, VAST's [Python bindings](/docs/use/integrate/python) use this format
for high-bandwidth data exchange.

:::note Extension Types
VAST translates its own types into Arrow [extension
types](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
to properly describe domain-specific concepts like IP addresses or subnets.
VAST's [Python bindings][vast-python] come with the required tooling, so you can
work with native types instead of relying on generic string or number
representations.
:::

## Input

The `import arrow` command imports [`Arrow IPC`][arrow-ipc] data. Since this
format carries the schema alongside the data, `import arrow` is self-contained
and does not require an additional schema.

To demonstrate how the `arrow` format works, consider this snippet of Python
that generates an Arrow IPC stream to stdout:

```python title=generate.py
import pyarrow as pa
import sys

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]

batch = pa.record_batch(data, names=['a', 'b', 'c'])

sink = pa.output_stream(sys.stdout.buffer)
with pa.ipc.new_stream(sink, batch.schema) as writer:
   for i in range(5):
      writer.write_batch(batch)
```

You can import the output of this script into VAST as follows:

```bash
python generate.py | vast import arrow
```

:::caution Limited Compatibility
VAST can currently only import Arrow data that it also exported. We are working
on support for processing arbitrary Arrow data.
:::

## Output

Since Arrow IPC is self-contained and includes the full schema, you can use it
to transfer data between VAST nodes, even if the target node is not aware of the
underlying schema.

To export a query result as an Arrow IPC stream, use `export arrow`:

```bash
vast export arrow '1.2.3.4 || #type == "suricata.alert"'
```

Note that this generates binary output. Make sure you pipe the output to a tool
that reads an Arrow IPC stream on stdin.

VAST's [Python bindings][vast-python] use this method to retrieve data from a
VAST server.

[arrow-ipc]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
[vast-python]: /docs/use/integrate/python
