PyVAST - VAST Python CLI Wrapper
================================

With `pyvast` we provide a very minimal python wrapper around the VAST command
line interface. The wrapper features fluent method chaining and works
asynchronously.

All VAST commands can be used with the wrapper. However, the wrapper does not
implement any commands itself. It simply passes all received arguments to the
`vast` binary. It is hence very easy to make mistakes in form of typos, given
this minimalistic implementation. Please refer to the [vast
documentation](https://vast.io) for details about valid `vast` commands.

## Installation

Use `pip` to install the package.

```sh
virtualenv --system-site-packages venv
source venv/bin/activate
pip install pyvast
```

## Usage

Commands are simply chained via `.`-notation. Parameters can be passed as python
keyword arguments. The following examples provide an overview of VAST commands
and the analogous `pyvast` commands.

- Query for an IP address and return 10 results in JSON
  ```sh
  # CLI call
  vast export --max-event=10 json ':addr == 192.168.1.104'
  ```
  ```py
  # python wrapper
  proc = await vast.export(max_events=10).json("192.167.1.102").exec()
  stdout, stderr = await proc.communicate()
  print(stdout)
  ```
- Import a Zeek log file
  ```sh
  # CLI call
  vast import zeek --read=/path/to/file
  ```
  ```py
  # python wrapper
  proc = await vast.import_().zeek(read="/path/to/file").exec()
  stdout, stderr = await proc.communicate()
  print(stdout)
  ```

### Module Parameterization

You can use PyVAST as Python module. After installing it via `pip`, simply
import it normally in your Python application.

```py
from pyvast import VAST
```

Once imported, there are four optional keyword arguments to instruct PyVAST
with: `binary`, `endpoint`, `container` and `logger`. The `binary` keyword
defaults to `"vast"`. In case the `vast` binary is not in your `$PATH`, set this
to the actual path to the VAST binary. If no `vast` binary is found, PyVAST
attempts to run VAST client commands in a container environment instead,
assuming an already running VAST container as specified in the `container` input
(defaulting to `{"runtime: "docker", "name": "vast"}`). The `endpoint` keyword
refers to the endpoint of the VAST node (e.g., `localhost:42000`).

Lastly, use the `logger` keyword to provide a custom
[logging.logger](https://docs.python.org/3/library/logging.html#logger-objects)
object for your application.

See also the full example below.

### Full Example

The following example shows a minimalistic working example with all required
import statements.

```py
#!/usr/bin/env python3

import asyncio
from pyvast import VAST

async def example():
  vast = VAST(binary="/opt/tenzir/bin/vast")
  await vast.test_connection()

  proc = await vast.export(max_events=10).json("192.168.1.103").exec()
  stdout, stderr = await proc.communicate()
  print(stdout)

asyncio.run(example())
```

See also the `example` folder for a demo using `pyarrow` for data export and a
demo for continuous queries.

## Testing

The tests are written with the python
[unittest](https://docs.python.org/3/library/unittest.html) library and its
asynchronous analogon [aiounittest](https://pypi.org/project/aiounittest/).
Install the `requirements.txt` first to run the tests.

```sh
pip install --user -r requirements.txt
python -m unittest discover .
```

## Development

Use the `setup.py` for local installation of a development setup.

```sh
virtualenv --system-site-packages venv # create a virtual env
source venv/bin/activate
python setup.py develop # or python setup.py install
```
