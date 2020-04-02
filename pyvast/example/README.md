PyVAST Example
==============

This example demonstrates how to interact with vast via the python bridge.
Install the `requirements.txt` first, before executing the script.

```sh
virtualenv --system-site-packages venv
source venv/bin/activate
pip install -r requirements.txt
```

Next, a running `vast` instance is required. Follow the [installation
instructions](https://github.com/tenzir/vast/blob/master/INSTALLATION.md) in
case you haven't done so already. Then start `vast` and import some data.

```sh
vast start
```

Run the `example.py` in another terminal.

```py
python example.py
```
