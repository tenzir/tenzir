PyVAST - VAST Python CLI Wrapper
================================

With `pyvast` we provide a very minimal python wrapper around the VAST command
line interface. The wrapper features fluent method chaining and works
asynchronously.

All VAST commands can be used with the wrapper. However, the wrapper does not
implement any commands itself. It simply passes all received arguments to the
`vast` binary. It is hence very easy to make mistakes in form of typos, given
this minimalistic implementation. Please refer to the
[vast documentation](https://docs.tenzir.com/) for details about valid `vast`
commands.

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
  stdout, stderr = vast.export(max_events=10).json("192.167.1.102").exec()
  print(stdout)
  ```
- Import a Zeek log file
  ```sh
  # CLI call
  vast import zeek --read=/path/to/file
  ```
  ```py
  # python wrapper
  stdout, stderr = vast.import_().zeek(read="/path/to/file").exec()
  print(stdout)
  ```

#### Full Example

The following example shows a minimalistic working example with all required
import statements.

```py
#!/usr/bin/env python3

import asyncio
from pyvast import VAST

vast = VAST(binary="/opt/tenzir/bin/vast")
asyncio.run(vast.test_connection())

stdout, stderr = asyncio.run(vast.export(max_events=10).json("192.167.1.102").exec())
print(stdout)
```

See also the `example` folder for a demo using `pyarrow` for data export.

## Testing

The tests are written with the python
[unittest](https://docs.python.org/3/library/unittest.html) library and its
asynchronous analogon [aiounittest](https://pypi.org/project/aiounittest/).
Install the `requirements.txt` first to run the tests.

```sh
pip install --user -r requirements.txt
python -m unittest discover .
```

## Installation

Use the `setup.py` for installation or development setup.

```sh
virtualenv --system-site-packages venv # create a virtual env
source venv/bin/activate
python setup.py develop # or python setup.py install
```
