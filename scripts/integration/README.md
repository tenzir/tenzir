# Integration Tests

This directory contains a tailor-made integration test utility for VAST. Unlike
the *unit tests*, which perform isolated tests of individual components, the
*integration tests* simulate real-world uses of the system and verify its
expected behavior.

Integration tests are YAML files that follow a specific schema.

## Test Specification Schema

Each *test* consists of a *name* (1), an optional list of *tags* (2), an
optional reference to a *fixture* (3), and a sequence of *steps* (4) that
consists of an application *command* (5), an optional *input* dataset in `gz`
format (6) and an optional *reference* file (7) for comparing the output of
`command`.

Example:

``` yaml
Zeek conn log: (1)
  tags: [zeek, example]
  fixture: ExampleTester (2)
  steps: (3)
    - command: import -b zeek (4)
      input: data/zeek/conn.log.gz (5)
    - command: export ascii 'resp_h == 192.168.1.104' (4)
      reference: reference/conn_export_1.ascii (6)
```

Fixtures are associative arrays with the mandatory keys `enter` and `exit`.
Both their values are expected to be written as block form scalars containing
Python code. This code executes within `Tester.run()` as setup and tear-down
hook.

Example:

``` yaml
fixtures:
  ExampleTester:
    enter: | # python
      print('The current working directory is {}'.format(work_dir))
    exit: | # python
      pass
```

The variable `work_dir` contains the working directory with the VAST logs of the
current test.

The creation of a subprocess should be done through the `spawn()` wrapper
function, which passes its arguments to the `Popen` constructor from `subprocess`.

A *test set* is a collection of tests and fixtures in one YAML file.
The default test set resides in `default_set.yaml`.

## Test Runner

### Prerequisites

The runner depends on the python libraries `schema` and `pyyaml`. You can make
them available in a temporary environment:

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### Usage

Still in the virtual environment, you can now run the default test set:

```sh
python3 integration.py --app <path/to/core>
```

You can get an overview of all available options by running

```sh
python3 integration.py --help
```

During the execution of a test, each step is invoked after an error free run of
the previous step. An error is detected if the return code of the command is
not `0`. A step is successful if no error occurred and if the stdout output of
`command` matches the reference.

### Output

The runner prints status information about the current test and step as the
tests are executed. At the end of each test a triplet of numbers that
represents a summary is printed. The format is:

```
(number of steps defined/count of successful steps/cound of failed steps)
```

A test is successful if all steps are completed successfully.

Example output:

```sh
Test: Zeek conn log
Waiting for port 42000 to be available
Starting server
Waiting for server to listen on port 42000
Running step ['import', 'zeek']
Running step ['export', 'ascii', 'resp_h == 192.168.1.104']
Shutting down server
(2/2/0)
```

If the output of a step produces a mismatch with the provided reference, a diff
between them is printed to `stdout`.
