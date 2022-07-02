---
sidebar_position: 0
---

# Install

Threat Bus is written in Python and ships as a [PyPI](https://pypi.org/)
package. Plugins are packaged individually and also available via PyPI. This
separation keeps the Threat Bus host clean from unnecessary dependencies.
Everything can be installed via `pip`, independent of the underlying OS.

## Setup a Virtual Environment

It may be desirable to install Threat Bus and its plugins in a
[virtual environment](https://docs.python.org/3/tutorial/venv.html). Set it up
as follows.

```bash
python -m venv --system-site-packages venv
source venv/bin/activate
```

:::note Python Version Requirements
Threat Bus requires at least Python 3.7, earlier versions are not supported.
:::
:::tip
Some plugins might require libraries that cannot be installed via `pip`, i.e.,
the `threatbus-zeek` plugin requires Broker as a dependency, which can only
installed system-wide together with the Broker library itself.

Hence we recommend passing the flag `--system-site-packages` when setting up the
Python `venv`. That allows threatbus to access libraries installed in system
scope.
:::

## Install Threat Bus and Plugins

Use [pip](https://pypi.org/project/pip/) to install Threat Bus and some plugins.

```bash
# core functionality & runtime:
pip install threatbus

# general naming convention:
pip install threatbus-<plugin-name>

# application plugins:
pip install threatbus-misp
pip install threatbus-zeek
pip install threatbus-vast
pip install threatbus-cif3
pip install threatbus-zmq

# backbone plugins:
pip install threatbus-inmem
pip install threatbus-rabbitmq
```

:::tip Local User Installation
You can install Python packages locally for your current user by specifying
`pip install --user <package>`
:::

Once installed, you can use `threatbus` as a stand-alone application via the
CLI. Print the help text as follows.

```bash
threatbus --help
```
