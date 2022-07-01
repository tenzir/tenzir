# Python

VAST ships with Python bindings to enable interaction with VAST in the Python
ecosystem. We distribute the bindings as [PyPI
package](https://pypi.org/project/pyvast/) called
[PyVAST](https://github.com/tenzir/vast/tree/master/pyvast).

## Install the PyPI package

Use `pip` to install PyVAST:

```bash
pip install pyvast
```

## Use PyVAST

PyVAST has a [asyncio](https://docs.python.org/3/library/asyncio.html)-based
wrapper around VAST's command line interface that uses fluent method chaining.
PyVAST supports all VAST commands by passing arguments to the `vast` exectuable.

Every command line invocation has an equivalent Python-native
invocation of chained (sub-)commands via the `.`-notation. You can pass
arguments as via Python's `*args` and parameters as `**kwargs`. When you are
done chaining methods, finalize the command invocation with a call to `.exec()`.

Here are two examples.

### Import a log file
  
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="Python" label="Python" default>

```py
proc = await vast.import_().zeek(read="/path/to/file").exec()
stdout, stderr = await proc.communicate()
print(stdout)
```

NB: since `import` is a reserved keyword, we add `_` as suffix.

</TabItem>
<TabItem value="CLI" label="CLI">

```bash
vast import --read=/path/to/file zeek
```

</TabItem>
</Tabs>

### Run a query

<Tabs>
<TabItem value="Python" label="Python" default>

```py
proc = await vast.export(max_events=10).json("192.167.1.102").exec()
stdout, stderr = await proc.communicate()
print(stdout)
```

</TabItem>
<TabItem value="CLI" label="CLI">

```bash
vast export --max-events=10 json 192.168.1.104
```

</TabItem>
</Tabs>

## Use PyVAST as module

You can use PyVAST as Python module:

```py
from pyvast import VAST
```

Once imported, there are three optional keyword arguments to instruct PyVAST
with:

- `binary` (default: `vast`): the path to the VAST executable. In case the
  VAST binary is not in your `$PATH`, set this to the actual path to the VAST
  binary.

- `endpoint` (default: `localhost:42000`): the endpoint of the VAST node.

- `logger` (optional): a custom [logging.logger][logger] object for your
  application.

[logger]: https://docs.python.org/3/library/logging.html#logger-objects

The following example shows a minimalistic working example with all required
import statements.

```py
#!/usr/bin/env python3

import asyncio
from pyvast import VAST

async def example():
  vast = VAST(binary="/opt/vast/bin/vast")
  await vast.test_connection()

  proc = await vast.export(max_events=10).json("192.168.1.103").exec()
  stdout, stderr = await proc.communicate()
  print(stdout)

asyncio.run(example())
```

The [PyVAST example directory][example] illustrates another use case involving
reading data via Arrow and running a continuous query.

[example]: https://github.com/tenzir/vast/tree/master/pyvast/example
