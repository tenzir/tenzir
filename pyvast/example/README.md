PyVAST Example
==============

This example demonstrates how to interact with vast via the python bridge.
Follow the steps below to setup a local `vast` node and ingest some demo data.

1. [Install VAST](https://github.com/tenzir/vast/blob/master/INSTALLATION.md)
2. `vast start` (start a vast node)
3. Go to another terminal, navigate to the `pyvast` folder
4. `curl -L -o - https://github.com/tenzir/vast/raw/master/integration/data/zeek/conn.log.gz | gunzip | vast import zeek` (ingest an example Zeek log)
5. `virtualenv --system-site-packages venv` (create a virtual env)
6. `source venv/bin/activate` (use the virtual env)
7. `python -m pip install -r example/requirements.txt` (install requirements for this example)
8. `python setup.py develop` (setup pyvast locally)
9. `python examples/exmaple.py` (run the example)
