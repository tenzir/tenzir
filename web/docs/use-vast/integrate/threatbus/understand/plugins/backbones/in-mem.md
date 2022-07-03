# In-Memory Backbone Plugin

The Threat Bus In-Mem plugin provides an in-memory backbone for data
provisioning. It is very simplistic and can be installed without any
dependencies.

The plugin only implements the minimal [backbone
specs](https://github.com/tenzir/threatbus/blob/master/threatbus/backbonespecs.py)
for Threat Bus backbone plugins.

## Installation

Install the In-Mem backbone plugin via `pip`.

```bash
pip install threatbus-inmem
```

## Configuration

The plugin does not require any configuration. Add an empty placeholder to the
Threat Bus `config.yaml` file.

```yaml
...
plugins:
  backbones:
    inmem:
...
```
