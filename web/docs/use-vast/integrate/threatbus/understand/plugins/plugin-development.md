# Plugin Development

This page provides a simply overview of the steps necessary for plugin
development. We recommend to use a [virtual
environment](https://docs.python.org/3/tutorial/venv.html) for all development
activities.

Clone the Threat Bus project, setup a virtual env, and install `threatbus` and
some plugins with the in development mode:

```
git clone https://github.com/tenzir/threatbus.git
cd threatbus
python -m venv --system-site-packages venv
source venv/bin/activate
make dev-mode
```

## Configuration & Extension

A plugin must define a `setup.py`. Whenever a plugin is installed, you have to
add a corresponding configuration section to `threatbus`' `config.yaml`. That
section has to be named after the `name` in the entry-point declaration of the
plugin's `setup.py` file.

Please adhere to the [plugin naming
conventions](https://pluggy.readthedocs.io/en/latest/#a-complete-example)
proposed by [pluggy](https://pypi.org/project/pluggy/) and always prefix your
plugin name with `threatbus-`.

Plugins can either be apps or backbones. Application plugins add new
functionality to Threat Bus and allow communication with applications that
consume or produce security content (e.g., Zeek or Suricata). Backbone plugins
add a new storage and distribution backend to Threat Bus (e.g., in-memory or
RabbitMQ).

Consider the following example setup:

- Plugin folder structure:
  ```bash
  plugins
  ├── apps
  |   └── threatbus_myapp
  │       ├── setup.py
  |       └── threatbus_myapp
              └── plugin.py
  └── backbones
      └── threatbus_mybackbone
          ├── setup.py
          └── threatbus_mybackbone
              └── plugin.py
  ```
- The `setup.py` file for a new plugin call `myapp`
  ```py
  from setuptools import setup
  setup(
    name="threatbus-myapp",
    install_requires="threatbus",
    entry_points={"threatbus.app": ["myapp = threatbus_myapp.plugin"]},
    packages=["threatbus_myapp"],
  )
  ```
- The corresponding `config.yaml` entry for the new plugin
  ```yaml
  ...
  plugins:
    apps:
      myapp:
        some-property: some-value
  ```

The `setup.py` file for the backbone plugin would look similar, except that the
`entrypoint` declaration must instead refer to `threatbus.backbone` instead of
`threatbus.app`.

### Implementation Specs

Threat Bus uses [pluggy](https://pypi.org/project/pluggy/) for plugin
management. Hence, users must implement the
[hookspecs](https://pluggy.readthedocs.io/en/latest/#implementations) defined in
the Threat Bus core project. Think of `hookspecs` as an interface definition
for plugins.

Find these plugin specifications in
[threatbus/appspecs.py](https://github.com/tenzir/threatbus/blob/master/threatbus/appspecs.py)
and
[threatbus/backbonespecs.py](https://github.com/tenzir/threatbus/blob/master/threatbus/backbonespecs.py).
For any plugin, you should at least implement the `run` function.

#### Stoppable Workers

Threat Bus plugins are encouraged to use Python threads, so a busy plugin can
never block the main thread and Threat Bus stays operational. For that, we offer
the
[StoppableWorker](https://github.com/tenzir/threatbus/blob/master/threatbus/stoppable_worker.py)
base-class to model plugin's busy work. Implementing that class also facilitates
a graceful shutdown. Please use this class when implementing your own Threat Bus
plugin.

All officially maintained Threat Bus plugins implement `StoppableWorker`. Refer
to any of the existing plugins for an example.
