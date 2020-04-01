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
  stdout, stderr = vast._import().zeek(read="/path/to/file").exec()
  print(stdout)
  ```


## Testing

The tests are written with the python
[unittest](https://docs.python.org/3/library/unittest.html) library and its
asynchronous analogon [aiounittest](https://pypi.org/project/aiounittest/).
Install the `requirements.txt` first to run the tests.

```sh
pip install --user -r requirements.txt
python -m unittest discover .
```
