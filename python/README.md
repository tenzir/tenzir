# VAST Python

The Python package of VAST provides a flexible control plane to integrate VAST
with other security tools.

> **Note**
> The Python effort is still highly experimental and subject to rapid change.
> Please do not consider it for production use.

## Usage

To get started, clone the VAST repository and install the Python package via
[Poetry](https://python-poetry.org/docs/):

```bash
git clone https://github.com/tenzir/vast.git
cd vast/python
poetry install --all-extras
```

Thereafter, you should be able to run files in the `examples` directory, e.g.,
via a Poetry shell:

```bash
poetry shell
python examples/misp.py
```

## Development

We recommend that you work with an editable installation, which is the default
for `poetry install`.

### Unit Tests

Run the unit tests via pytest:

```bash
poetry run pytest
```

## Packaging

The following instructions concern maintainers who want to publish the Python
package to PyPI.

### Setup Test PyPI

1. Add a Test PyPi repository:

   ```bash
   poetry config repositories.test-pypi https://test.pypi.org/legacy/
   ```

2. Get the token from <https://test.pypi.org/manage/account/token/>.

3. Store the token:

  ```bash
  poetry config pypi-token.test-pypi  pypi-YYYYYYYY
  ```

### Setup Production PyPI

1. Get the token from <https://pypi.org/manage/account/token/>.

2. Store the token:

  ```bash
  poetry config pypi-token.pypi pypi-XXXXXXXX
  ```

### Publish to PyPI

1. Publish to Test PyPI:
  
   ```bash
   poetry publish -r test-pypi
   ```

2. If everything went well, publish to PyPi:

   ```bash
   poetry publish --build
   ```
