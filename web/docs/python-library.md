# Python Library

Tenzir ships with a Python library to enable interaction with Tenzir with
primitives that add Tenzir's extension types to Apache Arrow. We distribute the
library as PyPI package called [tenzir][pypi-page].

The library is bundled with every Tenzir installation, and used internally for
the `python` operator.

[pypi-page]: https://pypi.org/project/tenzir/

:::warning Experimental
The Python library is considered experimental and subject to change without
notice.
:::

## Install the PyPI package

Use `uv` to install Tenzir:

```bash
uv pip install tenzir[module]
```
