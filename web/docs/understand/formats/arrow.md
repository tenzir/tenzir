---
description: Columnar format for analytics workloads
---

# Arrow

VAST supports reading and writing data in the binary [`Arrow IPC`][arrow-ipc]
columnar format, suitable for efficient handling of large data sets. For
example, VAST's [Python bindings](../../use/integrate/python.md) use this format
for high-bandwidth data exchange.

:::note Extension Types
VAST translates its own types into Arrow [extension
types](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
to properly describe domain-specific concepts like IP addresses or subnets.
VAST's [Python bindings][vast-python] come with the required tooling, so you can
work with native types instead of relying on generic string or number
representations.
:::

## Parser

The `import arrow` command imports [`Arrow IPC`][arrow-ipc] data. This allows
for efficiently transferring data between VAST nodes:

```bash
VAST_SOURCE_HOST=localhost:5158
VAST_DESTINATION_HOST=localhost:42001

# Transfer all Zeek events from the VAST node at VAST_SOURCE_HOST to the VAST
# node at VAST_DESTINATION_HOST.
vast --endpoint=${VAST_SOURCE_HOST} export arrow '#type == /zeek.*/' \
  | vast --endpoint=${VAST_DESTINATION_HOST} import arrow
```

:::caution Limited Compatibility
Technically, this format carries the schema alongside the data: `import arrow`
is self-contained and does not require an additional schema. However, the Arrow
import is currently limited to Arrow data that was exported by VAST via the
`export arrow` command. We plan to remove this restriction in the future,
allowing the following Python code to work:

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

```bash
python generate.py | vast import arrow
```
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
[vast-python]: ../../use/integrate/python.md
